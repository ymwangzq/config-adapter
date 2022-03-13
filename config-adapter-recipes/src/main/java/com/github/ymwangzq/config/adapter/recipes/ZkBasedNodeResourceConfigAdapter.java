package com.github.ymwangzq.config.adapter.recipes;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.zookeeper.GenericZkBasedNodeBuilder;
import com.github.phantomthief.zookeeper.ZkBasedNodeResource;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.github.ymwangzq.config.adapter.core.BaseEventSource;
import com.github.ymwangzq.config.adapter.core.ConfigAdapters;
import com.github.ymwangzq.config.adapter.core.GcUtil;
import com.github.ymwangzq.config.adapter.core.RootConfigAdapterHelper;
import com.github.ymwangzq.config.adapter.core.SingleThreadConfigDispatchExecutor;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;

/**
 * @author myco
 * Created on 2019-10-23
 */
@ThreadSafe
public class ZkBasedNodeResourceConfigAdapter extends RootConfigAdapterHelper {

    private static final Logger logger = LoggerFactory.getLogger(ZkBasedNodeResourceConfigAdapter.class);

    private static final int MAX_FETCH_TIMEOUT_MS = (int) TimeUnit.MINUTES.toMillis(1);
    // 首次拉取配置的默认超时限制，调大到 10s
    private static final int DEFAULT_FETCH_TIMEOUT_MS = 10_000;

    public static int getDefaultFetchTimeoutMs() {
        return DEFAULT_FETCH_TIMEOUT_MS;
    }

    @Nonnull
    public static <T> ConfigAdapter<T> create(@Nonnull GenericZkBasedNodeBuilder<T> builder,
                                              @Nonnull ExecutorService initExecutor, int initFetchTimeoutMs) {
        return create(builder, (newValue, oldValue) -> { }, initExecutor, initFetchTimeoutMs);
    }

    @Nonnull
    public static <T> ConfigAdapter<T> create(@Nonnull GenericZkBasedNodeBuilder<T> builder,
                                              @Nonnull BiConsumer<? super T, ? super T> onChangeListener, @Nonnull ExecutorService initExecutor,
                                              int initFetchTimeoutMs) {
        return createConfigAdapter(builder, onChangeListener, initExecutor, initFetchTimeoutMs);
    }

    /**
     * @param initExecutor 请避免传入 newDirectExecutorService(), 因为这样会导致超时限制失效
     */
    private static <T> ConfigAdapter<T> createConfigAdapter(@Nonnull GenericZkBasedNodeBuilder<T> builder,
                                                            @Nonnull BiConsumer<? super T, ? super T> onChangeListener, @Nonnull ExecutorService initExecutor,
                                                            int initFetchTimeoutMs) {

        Preconditions.checkNotNull(builder);
        Preconditions.checkNotNull(onChangeListener);
        Preconditions.checkNotNull(initExecutor);
        Preconditions.checkArgument(initFetchTimeoutMs > 0 && initFetchTimeoutMs <= MAX_FETCH_TIMEOUT_MS);

        BaseEventSource eventSource = new BaseEventSource();

        // 这里搞个独立线程来触发更新事件，避免有人传入了可能无限阻塞的 mapper/cleanup 导致阻塞 zk 的通知线程
        // 应该也不占啥资源
        SingleThreadConfigDispatchExecutor eventDispatchExecutor =
                createEventDispatchExecutor("ZkBasedNodeResourceConfigAdapter-event-dispatch-%d");

        ZkBasedNodeResource<T> zkBasedNodeResource = builder.onResourceChange((newValue, oldValue) -> {
            if (newValue != oldValue) {
                logger.debug("dispatch value: new: [{}], old: [{}]", newValue, oldValue);
                dispatchUpdateEvent(eventSource, eventDispatchExecutor);
            }
            onChangeListener.accept(newValue, oldValue);
        }).build();

        Supplier<T> fetcher = new Supplier<T>() {

            private volatile boolean initSuccess = false;
            private volatile Future<T> initFuture = null;

            @Override
            public T get() {
                // BaseConfigAdapter 保证了 fetcher 不会有并发执行, initSuccess 就不用原子变量了
                if (!initSuccess) {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    // 头一次读 zk 的时候, 可能有连不上、超时之类的问题, 所以加个超时限制
                    try {
                        if (initFuture == null) {
                            initFuture = initExecutor.submit(zkBasedNodeResource::get);
                        }
                        T value = initFuture.get(initFetchTimeoutMs, TimeUnit.MILLISECONDS);
                        initSuccess = true;
                        initFuture = null;
                        return value;
                    } catch (TimeoutException t) {
                        // future get 超时，保留 initFuture 不变，以便下次 get 复用
                        throw new RuntimeException("从 zk 读配置异常, 耗时: " + stopwatch.elapsed(), t);
                    } catch (Throwable t) {
                        // 其他异常，把 initFuture 清理掉，下次 get 重新调用
                        initFuture = null;
                        throw new RuntimeException("从 zk 读配置异常，耗时: " + stopwatch.elapsed(), t);
                    }
                } else {
                    // 后续都是直接从内存读, 直接 get 就行, 不会超时
                    // 而且, 初始化的时候会触发 ZkBasedNodeResource 的 onChangeListener
                    // 如果不在同一个线程上执行, 会导致在 com.github.ymwangzq.config.adapter.core.BaseConfigAdapter#update
                    // 上边死锁; 所以, 只有第一次初始化提交到线程池并限制超时, 后续的更新都在 caller thread 上做
                    return zkBasedNodeResource.get();
                }
            }
        };

        // 这里为了单元测试可以模拟更新
        registerRootEventDispatcher(eventSource);
        ConfigAdapter<T> configAdapter = ConfigAdapters.create(eventSource, fetcher, t -> { });

        // 如果壳对象被 gc 了，fetcher 对象才可能会被 gc，fetcher 被回收掉以后，就把 zkBasedNodeResource 给关了
        GcUtil.registerCleanUp(fetcher, () -> {
            eventDispatchExecutor.shutdown();
            zkBasedNodeResource.close();
        });
        return configAdapter;
    }
}
