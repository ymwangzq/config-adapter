package com.github.ymwangzq.config.adapter.core;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ymwangzq.config.adapter.facade.EventDispatcher;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;

/**
 * @author myco
 * Created on 2020-06-22
 */
public class SupplierConfigAdapter extends RootConfigAdapterHelper {

    private static final Logger logger = LoggerFactory.getLogger(SupplierConfigAdapter.class);

    private static final ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("SupplierConfigAdapter-change-checker").build());

    private static final Set<SupplierChangeHelper<?>> SUPPLIER_CHANGE_HELPERS = Sets.newConcurrentHashSet();

    @VisibleForTesting
    static long getSupplierChangeHelpersSize() {
        return SUPPLIER_CHANGE_HELPERS.size();
    }

    private static final Supplier<Void> INIT = Suppliers.memoize(() -> {
        SCHEDULER.scheduleWithFixedDelay(
                () -> SUPPLIER_CHANGE_HELPERS
                        .forEach(supplierChangeHelper -> {
                            try {
                                supplierChangeHelper.checkChange();
                            } catch (Throwable t) {
                                logger.warn("[fail-safe] supplierChangeHelper exception", t);
                            }
                        }),
                0,
                Math.min(1000, Math.max(10, Long.getLong("SupplierConfigAdapter.check.interval.ms", 1000))),
                TimeUnit.MILLISECONDS);
        return null;
    });

    private static class SupplierChangeHelper<T> implements Supplier<T> {
        private final Supplier<T> supplier;
        private final EventDispatcher eventDispatcher;
        private final SingleThreadConfigDispatchExecutor eventDispatchExecutor;

        private volatile T value;
        private volatile boolean activated = false;
        private final AtomicReference<Future<?>> lastDispatchFutureHolder = new AtomicReference<>();

        private SupplierChangeHelper(@Nonnull Supplier<T> supplier, @Nonnull EventDispatcher eventDispatcher,
                @Nonnull SingleThreadConfigDispatchExecutor eventDispatchExecutor) {
            Preconditions.checkNotNull(supplier);
            this.supplier = supplier;
            this.eventDispatcher = eventDispatcher;
            this.eventDispatchExecutor = eventDispatchExecutor;
        }

        @Override
        public T get() {
            // 这里只会由 BaseConfigAdapter 调用，不会有并发，所以这里不用额外做同步
            if (!activated) {
                // 先设置 value
                value = supplier.get();
                // 再设置 activated
                activated = true;
            }
            return value;
        }

        @Nullable
        private Future<?> checkChange() {
            Future<?> dispatchFuture = null;
            if (activated) {
                T newValue = supplier.get();
                T oldValue = value;
                value = newValue;
                if (oldValue != newValue) {
                    logger.trace("dispatch value: new: [{}], old: [{}]", newValue, oldValue);
                    dispatchFuture = dispatchUpdateEvent(eventDispatcher, eventDispatchExecutor);
                }
            }
            return dispatchFuture;
        }
    }

    /**
     * @param supplier 这里的 supplier 不能有副作用，会被定期调用检查是否有变更
     */
    public static <T> ConfigAdapter<T> createConfigAdapter(@Nonnull Supplier<T> supplier) {
        Preconditions.checkNotNull(supplier);
        INIT.get();

        BaseEventSource eventSource = new BaseEventSource();

        // 这里搞个独立线程来触发更新事件，避免有人传入了可能无限阻塞的 mapper/cleanup 导致阻塞上游的通知线程
        SingleThreadConfigDispatchExecutor eventDispatchExecutor =
                createEventDispatchExecutor("SupplierConfigAdapter-event-dispatch-%d");

        SupplierChangeHelper<T>
                supplierChangeHelper = new SupplierChangeHelper<>(supplier, eventSource, eventDispatchExecutor);
        SUPPLIER_CHANGE_HELPERS.add(supplierChangeHelper);

        // 这里为了单元测试可以模拟更新
        BaseEventSource eventSourceForUnitTest = new BaseEventSource();
        registerRootEventDispatcher(eventSourceForUnitTest);
        // 收到单测模拟的更新事件时，立刻发起一次变更检查
        eventSourceForUnitTest.addEventListener(event -> Optional.ofNullable(supplierChangeHelper.checkChange())
                .ifPresent(f -> {
                    try {
                        // 单测里边，给个1分钟超时时间吧
                        f.get(1, TimeUnit.MINUTES);
                    } catch (Throwable t) {
                        logger.warn("eventSourceForUnitTest wait timeout", t);
                    }
                }));

        ConfigAdapter<T> configAdapter =
                ConfigAdapters.createMapOnEachUpdate(eventSource, supplierChangeHelper, t -> { });

        // 如果壳对象被 gc 了，打个点看看有多少这样的用法
        GcUtil.registerCleanUp(configAdapter, () -> {
            SUPPLIER_CHANGE_HELPERS.remove(supplierChangeHelper);
            eventDispatchExecutor.shutdown();
        });
        return ConfigAdapters.toOnlyMapOnUpstreamChangeConfigAdapter(configAdapter);
    }
}
