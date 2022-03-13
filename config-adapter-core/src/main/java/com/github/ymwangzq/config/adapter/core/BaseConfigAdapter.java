package com.github.ymwangzq.config.adapter.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.github.ymwangzq.config.adapter.facade.AdaptiveCleanUpConfigAdapterMapper;
import com.github.ymwangzq.config.adapter.facade.AdaptiveCleanUpProxy;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapterMapper;
import com.github.ymwangzq.config.adapter.facade.DelayCleanUpConfig;
import com.github.ymwangzq.config.adapter.facade.EventListener;
import com.github.ymwangzq.config.adapter.facade.StatefulEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.github.ymwangzq.config.adapter.facade.EventSource;

/**
 * 支持延迟 cleanUp
 * 1）：只有下游所有 ConfigAdapter 都更新成功后，才会执行老值的 cleanUp
 * 2）：支持设置延迟多久执行 cleanUp
 * <p>
 * 支持合并多个配置源（监听多个配置变更）
 * <p>
 * BaseConfigAdapter 这个对象从来都不会直接暴露出去，永远都是被 DelegatingConfigAdapter 包起来后再暴露出去的
 *
 * @author myco
 * Created on 2019-10-11
 */
@ThreadSafe
public final class BaseConfigAdapter<T, C> implements ConfigAdapter<T> {

    private static final Logger logger = LoggerFactory.getLogger(BaseConfigAdapter.class);

    private static final ExecutorService FAIL_EVENT_DISPATCHER = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("config-adapter-fail-event-dispatcher").build());

    enum Stat {
        CREATED,
        INITIALIZING,
        ACTIVATED,
        DETACHED
    }

    @GuardedBy("fetchSwapCleanupLock")
    private volatile T ref = null;
    private final AtomicReference<Stat> stat = new AtomicReference<>(Stat.CREATED);

    private final EventSource upstreamEventSource;
    private final EventListener updateEventListener;
    private final EventListener propagateEventListener;

    private final BaseEventSource baseEventSource;
    private final Supplier<T> fetcher;
    private final Function<T, C> adaptiveCleanUpTransformer;
    private final Consumer<C> cleanUp;

    private final Object fetchSwapCleanupLock = new Object();

    // 这玩意是为了做延迟 cleanUp 的
    private final ConcurrentLinkedQueue<ValueHolder<T>> toBeCleanUp = new ConcurrentLinkedQueue<>();
    // 如果在下游 update 的时候失败了, 那当前的对象就不能做 cleanUp; 要等到某一次下游全部 update 成功以后, 才能做 cleanUp
    // 修改 canNotCleanUpUntilUpdateSuccess 之前需要加一个锁，因为这里是延迟的 cleanup，有可能在异步线程里执行。
    // 加锁避免不同线程里已经在执行清理 canNotCleanUpUntilUpdateSuccess 的线程把刚放进去的 value 也给清理掉
    @GuardedBy("canNotCleanUpUntilUpdateSuccess")
    private final ConcurrentLinkedQueue<ValueHolder<T>> canNotCleanUpUntilUpdateSuccess = new ConcurrentLinkedQueue<>();

    /**
     * 使用 ConfigAdapters.createMapOnEachUpdate
     */
    @Deprecated
    public static <O> ConfigAdapter<O> create(@Nonnull EventSource eventSource, @Nonnull Supplier<O> fetcher,
                                              @Nonnull Consumer<O> cleanUp) {
        return ConfigAdapters.createMapOnEachUpdate(eventSource, fetcher, cleanUp);
    }

    @Deprecated
    public static <O> ConfigAdapter<O> create(@Nonnull EventSource eventSource, @Nonnull Supplier<O> fetcher,
                                              @Nonnull Consumer<O> cleanUp, @Nullable DelayCleanUpConfig delayCleanUpConfig) {
        return ConfigAdapters.createMapOnEachUpdate(eventSource, fetcher, cleanUp, delayCleanUpConfig);
    }

    /**
     * 使用 ConfigAdapters.create
     */
    @Deprecated
    public static <O> ConfigAdapter<O> createOnlyMapOnUpstreamChange(@Nonnull EventSource eventSource,
                                                                     @Nonnull Supplier<O> fetcher, @Nonnull Consumer<O> cleanUp) {
        return ConfigAdapters.create(eventSource, fetcher, cleanUp);
    }

    @Deprecated
    public static <O> ConfigAdapter<O> createOnlyMapOnUpstreamChange(@Nonnull EventSource eventSource,
                                                                     @Nonnull Supplier<O> fetcher, @Nonnull Consumer<O> cleanUp,
                                                                     @Nullable DelayCleanUpConfig delayCleanUpConfig) {
        return ConfigAdapters.create(eventSource, fetcher, cleanUp, delayCleanUpConfig);
    }

    /**
     * create 方法返回一个壳，壳在 ConfigAdapter 内部没有引用（各种 eventListener 都不引用这个壳）；壳被 gc 掉的时候会触发
     * ConfigAdapter 内部的一些清理逻辑
     * 创建的 ConfigAdapter 任何变更事件都会通知下游，不管当前值是否真的发生变化
     */
    static <O, P> ConfigAdapter<O> doCreate(@Nonnull EventSource eventSource, @Nonnull Supplier<O> fetcher,
                                            @Nonnull Function<O, P> adaptiveCleanUpTransformer, @Nonnull Consumer<P> cleanUp) {
        BaseConfigAdapter<O, P> core =
                new BaseConfigAdapter<>(eventSource, fetcher, adaptiveCleanUpTransformer, cleanUp, null);
        ConfigAdapter<O> wrapper = new DelegatingConfigAdapter<>(core);
        GcUtil.registerCleanUp(wrapper, core::detach);
        ConfigAdapterInitializer.registerInitUntilSuccess(wrapper);
        return wrapper;
    }

    static <O> ConfigAdapter<O> doCreate(@Nonnull EventSource eventSource, @Nonnull Supplier<O> fetcher,
                                         @Nonnull Consumer<O> cleanUp, @Nullable DelayCleanUpConfig delayCleanUpConfig) {
        BaseConfigAdapter<O, O> core =
                new BaseConfigAdapter<>(eventSource, fetcher, Functions.identity(), cleanUp, delayCleanUpConfig);
        ConfigAdapter<O> wrapper = new DelegatingConfigAdapter<>(core);
        GcUtil.registerCleanUp(wrapper, core::detach);
        ConfigAdapterInitializer.registerInitUntilSuccess(wrapper);
        return wrapper;
    }


    /**
     * 这个方法仅限于 create 内调用，其他任何地方都应使用 create 方法
     *
     * @param fetcher 严禁包含可能无限阻塞的操作，必须要在有限时间内返回或者抛异常，几秒钟已经顶天了
     * @param adaptiveCleanUpTransformer 用于获取对象内部真正需要 cleanUp 的对象，作为 cleanUp 方法的输入；抽出这个参数的意义见
     * {@link AdaptiveCleanUpProxy} 里的说明
     * @param cleanUp 严禁包含可能无限阻塞的操作，有需要阻塞的提交到外部线程中执行
     */
    private BaseConfigAdapter(@Nonnull EventSource eventSource, @Nonnull Supplier<T> fetcher,
                              @Nonnull Function<T, C> adaptiveCleanUpTransformer, @Nonnull Consumer<C> cleanUp,
                              @Nullable DelayCleanUpConfig delayCleanUpConfig) {

        Preconditions.checkNotNull(eventSource);
        Preconditions.checkNotNull(fetcher);
        Preconditions.checkNotNull(adaptiveCleanUpTransformer);
        Preconditions.checkNotNull(cleanUp);

        this.baseEventSource = new BaseEventSource();
        this.fetcher = fetcher;
        this.adaptiveCleanUpTransformer = adaptiveCleanUpTransformer;
        this.cleanUp = cleanUp;

        this.upstreamEventSource = eventSource;
        this.updateEventListener = EventListeners.updateEventListener(() -> {
            Stat currentStat = stat.get();
            if (!Stat.CREATED.equals(currentStat)) {
                // 如果当前状态都还没开始初始化，那也不需要处理上游的 update event，做到 lazy
                fetchSwapCleanUp(false);
            }
        }, MoreObjects.toStringHelper(this)
                .add("fetcher", fetcher)
                .add("cleanUp", cleanUp)
                .toString());
        this.propagateEventListener = EventListeners.propagateEventListener(baseEventSource,
                event -> {
                    // 如果这次分发的是 update event, 在下游都处理完以后, 再执行下 doDelayCleanUp
                    if (StatefulEvent.EventType.UPDATE.equals(event.getEventType())) {
                        if (delayCleanUpConfig != null) {
                            delayCleanUpConfig.delayCleanUpExecutor()
                                    .schedule(() -> doDelayCleanUp(event.getEventStat()),
                                            delayCleanUpConfig.cleanUpDelayMs().getAsLong(), TimeUnit.MILLISECONDS);
                        } else {
                            doDelayCleanUp(event.getEventStat());
                        }
                    }
                });

        // 处理 update event
        eventSource.addEventListener(updateEventListener);
        // 自己处理完了，再通知下游
        eventSource.addEventListener(propagateEventListener);
    }

    // ConcurrentLinkedQueue 里不让放 null, 没办法包一个对象吧
    private static class ValueHolder<T> {
        private final T value;

        private ValueHolder(T value) {
            this.value = value;
        }

        private T getValue() {
            return value;
        }
    }

    // 只有 update event 才可能触发 delay clean up
    // 一个 update event 可能产生 0 或 1 个需要 delay clean up 的对象 (取决于当前状态是否已经开始初始化)
    // 所以这里每次 poll 出来有可能是 null
    // 不过由于 event 只能单线程分发 (BaseEventSource.dispatchEvent 里加了锁)，所以仍然对整体效果并没什么影响
    private void doDelayCleanUp(StatefulEvent.EventStat eventStat) {
        ValueHolder<T> valueHolder = toBeCleanUp.poll();
        if (valueHolder != null) {
            // 如果这次 update 的下游有失败的, 那就把当前值丢到暂时不能 cleanUp 的队列里
            if (eventStat.hasStats(BaseEvent.EventStatImpl.EVENT_STAT_FETCH_FAIL)) {
                synchronized (canNotCleanUpUntilUpdateSuccess) {
                    canNotCleanUpUntilUpdateSuccess.offer(valueHolder);
                }
            } else {
                doCleanUp(valueHolder.getValue());
            }
        }
        // 如果这次 update 的下游全部更新成功了, 就可以 clean up 掉所有 canNotCleanUpUntilUpdateSuccess 里积攒的值了
        if (eventStat.isSuccess()) {
            List<ValueHolder<T>> needCleanUpValues = new ArrayList<>(canNotCleanUpUntilUpdateSuccess.size());
            synchronized (canNotCleanUpUntilUpdateSuccess) {
                // 先把 canNotCleanUpUntilUpdateSuccess 复制出来，因为 cleanup 逻辑是外部传入的，耗时不可控
                // 复制出来保证持有锁的时间可控且比较短
                Iterator<ValueHolder<T>> iterator = canNotCleanUpUntilUpdateSuccess.iterator();
                while (iterator.hasNext()) {
                    needCleanUpValues.add(iterator.next());
                    iterator.remove();
                }
            }
            for (ValueHolder<T> value : needCleanUpValues) {
                doCleanUp(value.getValue());
            }
        }
    }

    private void doCleanUp0(C valueToBeCleanUp) {
        try {
            cleanUp.accept(valueToBeCleanUp);
        } catch (Throwable t) {
            // 通知下游, 这里 cleanUp 失败了 (触发上报打点等)
            FAIL_EVENT_DISPATCHER.execute(() -> baseEventSource.dispatchEvent(BaseEvent.newCleanUpFailEvent()));
            throw new RuntimeException("cleanUp exception, " + cleanUp, t);
        }
    }

    private void doCleanUp(T valueToBeCleanUp) {

        C realValueToBeCleanUp = adaptiveCleanUpTransformer.apply(valueToBeCleanUp);

        if (realValueToBeCleanUp != valueToBeCleanUp) {
            try {
                if (realValueToBeCleanUp == null || !realValueToBeCleanUp.getClass()
                        .isAssignableFrom(valueToBeCleanUp.getClass())) {
                    logger.warn("AdaptiveCleanUpProxy 使用错误；fetcher: {}, cleanup: {}", fetcher, cleanUp);
                    doCleanUp0(realValueToBeCleanUp);
                } else {
                    GcUtil.registerCleanUp(valueToBeCleanUp, () -> doCleanUp0(realValueToBeCleanUp));
                }
            } catch (Throwable t) {
                logger.warn("AdaptiveCleanUpProxy 使用错误；fetcher: {}, cleanup: {}", fetcher, cleanUp);
            }
        } else {
            doCleanUp0(realValueToBeCleanUp);
        }
    }

    // 保存正在 or 即将 or 刚刚执行 fetcher 的「事件分发」线程，以便在 detach 的时候能快速中断，让「事件分发」线程能及时通知其他 EventListener
    private final AtomicReference<Thread> fetchingEventDispatcherThreadHolder = new AtomicReference<>();
    // 用来防止在 detach 的时候，把已经跑完 fetcher (开始更新其他 ConfigAdapter) 的「事件分发」线程给中断掉
    private final Object detachInterruptLock = new Object();

    /**
     * @param cleanUpImmediately 只有在事件分发线程里，cleanUpImmediately 才会是 false
     */
    private void fetchSwapCleanUp(boolean cleanUpImmediately) {
        //noinspection UnusedAssignment
        T oldValue = null;
        T newValue = null;
        try {
            if (!cleanUpImmediately) {
                // 事件分发线程触发时，保存一下事件分发线程
                fetchingEventDispatcherThreadHolder.set(Thread.currentThread());
            } else if (Stat.DETACHED.equals(stat.get())) {
                // 非事件分发线程 && 已经 DETACHED，说明是从 detach 方法进来的，就不用再进入下边的锁，多跑一遍 fetcher 了
                return;
            }
            synchronized (fetchSwapCleanupLock) {
                // 需要在锁保护下来取 oldValue, 否则初始化的时候可能拿到一个更老的值, 导致漏掉一个 oldValue 没有被 cleanup,
                // 同时有的 oldValue 会被 cleanup 多次
                oldValue = ref;
                try {
                    // fetcher.get 和 给 ref 赋值必须要保证原子性, fetcher.get 不能放到锁外
                    // 否则在快速更新多次的时候, 可能给 ref 设置上一个已经过期的值
                    if (Stat.DETACHED.equals(stat.get())) {
                        // 如果已经 DETACHED 就不用调用 fetcher 了，newValue 保持用 oldValue 就好
                        newValue = oldValue;
                    } else {
                        newValue = fetcher.get();
                    }
                    ref = newValue;
                    // ref 被赋值以后, 就算初始化完成了
                    // 更新 stat 需要放在锁内执行, 这样才能保证高并发场景下, 即使同时被多个线程初始化, 也只调用一次 fetcher
                    stat.compareAndSet(Stat.INITIALIZING, Stat.ACTIVATED);
                } catch (Throwable t) {
                    // 通知下游, 这里 fetch 失败了 (触发上报打点等)
                    FAIL_EVENT_DISPATCHER.execute(() -> {
                        // 这是我们内部维护的线程，清理 interrupt 标记，避免污染
                        //noinspection ResultOfMethodCallIgnored
                        Thread.interrupted();
                        baseEventSource.dispatchEvent(BaseEvent.newFetchFailEvent());
                    });
                    // fetcher 失败也就不需要做 cleanUp, 当前异常直接抛出去就行
                    throw new RuntimeException("fetcher exception, " + fetcher, t);
                } finally {
                    synchronized (detachInterruptLock) {
                        fetchingEventDispatcherThreadHolder.set(null);
                    }
                }
            }
            // 判断下新老 value 是否是同一个对象, 因为 fetcher 里很可能一直缓存着同一个对象
            // 同一个对象就不能做 cleanUp 了
            if (newValue != oldValue) {
                if (cleanUpImmediately) {
                    // oldValue 就算是 null 也执行下 cleanUp, 没准儿有啥高级用法
                    doCleanUp(oldValue);
                } else {
                    toBeCleanUp.offer(new ValueHolder<>(oldValue));
                }
            }
        } finally {
            if (Stat.DETACHED.equals(stat.get())) {
                synchronized (detachInterruptLock) {
                    Thread fetchingEventDispatcherThread = fetchingEventDispatcherThreadHolder.get();
                    // 这里如果 fetchingEventDispatcherThread 是 null，说明要么「事件分发线程」已经把 fetcher 跑完了
                    // 要么 「事件分发线程」再也不会执行 fetcher 了；所以 fetchingEventDispatcherThread 是 null 的情况不需要操心
                    if (fetchingEventDispatcherThread != null) {
                        logger.info("detach interrupt triggered");
                        // fetchingEventDispatcherThread 不是 null，说明「事件分发线程」正跑在 fetcher 附近，而且必定会等
                        // 下边的 interrupt 执行完，因为有 detachInterruptLock
                        fetchingEventDispatcherThread.interrupt();
                    }
                }
                // 然后再来清理当前 ref 里遗留的值
                synchronized (fetchSwapCleanupLock) {
                    // 锁内清理 ref，清理完后就置为 null，保证不会被 cleanUp 多次
                    T currentValue = ref;
                    ref = null;
                    doCleanUp(currentValue);
                }
                // 最后把之前积攒下来没做 cleanUp 的资源都清理掉
                doDelayCleanUp(new BaseEvent.EventStatImpl());
            }
        }
    }

    /**
     * 如果当前 ConfigAdapter 没用了，那就应该把他注册到其他 EventSource 上的 EventListener 都删掉，以便能够被 gc 回收掉
     * 然后再清理内部持有的资源
     * 在 BaseConfigAdapter.create 方法里使用，当发现外层壳对象被 gc 掉以后，会主动调用这个方法
     * <p>
     * 2021.12.03: 增加 close 方法支持主动 detach
     */
    private void detach() {
        if (!Stat.DETACHED.equals(stat.getAndSet(Stat.DETACHED))) {
            // 把上游的通知先断掉
            upstreamEventSource.removeEventListener(updateEventListener);
            upstreamEventSource.removeEventListener(propagateEventListener);

            // 通过 fetchSwapCleanUp 在锁内做清理，避免同时有更新事件导致 cleanup 多次等问题
            fetchSwapCleanUp(true);
        }
    }

    @Nullable
    @Override
    public T get() {
        // 初始化完成的就直接从 ref 取值就行了
        if (Stat.ACTIVATED.equals(stat.get())) {
            return ref;
        }
        if (Stat.DETACHED.equals(stat.get())) {
            throw new RuntimeException("ConfigAdapter already DETACHED!");
        }
        // 头一次初始化之前先做状态变更, 为了不丢失 update event
        if (Stat.CREATED.equals(stat.get())) {
            stat.compareAndSet(Stat.CREATED, Stat.INITIALIZING);
        }
        // 开始初始化, 加锁为了避免高并发场景下的多次初始化, 以及带来的其他问题
        synchronized (fetchSwapCleanupLock) {
            if (!Stat.ACTIVATED.equals(stat.get())) {
                fetchSwapCleanUp(true);
            }
        }
        // fetchSwapCleanup 执行过程中, 会把状态修改成 ACTIVATED
        // 没抛异常走到这里的话, ref 就是最新的有效值
        return ref;
    }

    @Nonnull
    @Override
    public <R> ConfigAdapter<R> map(@Nonnull ConfigAdapterMapper<? super T, R> configMapper) {
        if (Stat.DETACHED.equals(stat.get())) {
            throw new RuntimeException("ConfigAdapter already DETACHED!");
        }
        return doCreate(baseEventSource, new Supplier<R>() {
            @Override
            public R get() {
                return configMapper.map(BaseConfigAdapter.this.get());
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(this)
                        .add("configMapper", configMapper)
                        .toString();
            }
        }, configMapper::cleanUp, configMapper.delayCleanUpConfig());
    }

    @Nonnull
    @Override
    public <R, C1> ConfigAdapter<R> map(@Nonnull AdaptiveCleanUpConfigAdapterMapper<? super T, R, C1> configMapper) {
        if (Stat.DETACHED.equals(stat.get())) {
            throw new RuntimeException("ConfigAdapter already DETACHED!");
        }
        return doCreate(baseEventSource, new Supplier<R>() {
            @Override
            public R get() {
                return configMapper.map(BaseConfigAdapter.this.get());
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(this)
                        .add("configMapper", configMapper)
                        .toString();
            }
        }, configMapper::transformToRealCleanUp, configMapper::cleanUp);
    }

    @Override
    public void addEventListener(@Nonnull EventListener eventListener) {
        if (Stat.DETACHED.equals(stat.get())) {
            throw new RuntimeException("ConfigAdapter already DETACHED!");
        }
        baseEventSource.addEventListener(eventListener);
    }

    @Override
    public void removeEventListener(@Nonnull EventListener eventListener) {
        if (Stat.DETACHED.equals(stat.get())) {
            throw new RuntimeException("ConfigAdapter already DETACHED!");
        }
        baseEventSource.removeEventListener(eventListener);
    }

    Stat getStat() {
        return stat.get();
    }

    @Override
    public void close() {
        detach();
    }
}
