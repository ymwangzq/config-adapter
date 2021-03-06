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
            new ThreadFactoryBuilder().setNameFormat("SupplierConfigAdapter-change-checker").build());

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
            // ??????????????? BaseConfigAdapter ????????????????????????????????????????????????????????????
            if (!activated) {
                // ????????? value
                value = supplier.get();
                // ????????? activated
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
     * @param supplier ????????? supplier ????????????????????????????????????????????????????????????
     */
    public static <T> ConfigAdapter<T> createConfigAdapter(@Nonnull Supplier<T> supplier) {
        Preconditions.checkNotNull(supplier);
        INIT.get();

        BaseEventSource eventSource = new BaseEventSource();

        // ?????????????????????????????????????????????????????????????????????????????????????????? mapper/cleanup ?????????????????????????????????
        SingleThreadConfigDispatchExecutor eventDispatchExecutor =
                createEventDispatchExecutor("SupplierConfigAdapter-event-dispatch-%d");

        SupplierChangeHelper<T>
                supplierChangeHelper = new SupplierChangeHelper<>(supplier, eventSource, eventDispatchExecutor);
        SUPPLIER_CHANGE_HELPERS.add(supplierChangeHelper);

        // ??????????????????????????????????????????
        BaseEventSource eventSourceForUnitTest = new BaseEventSource();
        registerRootEventDispatcher(eventSourceForUnitTest);
        // ?????????????????????????????????????????????????????????????????????
        eventSourceForUnitTest.addEventListener(event -> Optional.ofNullable(supplierChangeHelper.checkChange())
                .ifPresent(f -> {
                    try {
                        // ?????????????????????1?????????????????????
                        f.get(1, TimeUnit.MINUTES);
                    } catch (Throwable t) {
                        logger.warn("eventSourceForUnitTest wait timeout", t);
                    }
                }));

        ConfigAdapter<T> configAdapter =
                ConfigAdapters.createMapOnEachUpdate(eventSource, supplierChangeHelper, t -> { });

        // ?????????????????? gc ?????????????????????????????????????????????
        GcUtil.registerCleanUp(configAdapter, () -> {
            SUPPLIER_CHANGE_HELPERS.remove(supplierChangeHelper);
            eventDispatchExecutor.shutdown();
        });
        return ConfigAdapters.toOnlyMapOnUpstreamChangeConfigAdapter(configAdapter);
    }
}
