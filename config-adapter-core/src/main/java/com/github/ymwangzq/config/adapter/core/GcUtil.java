package com.github.ymwangzq.config.adapter.core;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 这个类的目的是：
 * 如果有一些废弃的 ConfigAdapter，除了事件通知链路还有引用，没有其他任何引用时，这些废弃的 ConfigAdapter 以及相应的 EventListener 等对象能够被清理掉
 *
 * @author myco
 * Created on 2020-01-02
 */
public class GcUtil {

    private static final Logger logger = LoggerFactory.getLogger(GcUtil.class);

    private static final Multimap<PhantomReference<?>, Runnable> FINALIZER_MAP =
            Multimaps.newMultimap(Maps.newConcurrentMap(), ArrayList::new);
    private static final ReferenceQueue<Object> REFERENCE_QUEUE = new ReferenceQueue<>();

    private static final List<Runnable> FINALIZE_LISTENER_FOR_UT = Collections.synchronizedList(new ArrayList<>());

    private static final ScheduledExecutorService SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setNameFormat("config-adapter-gc-scheduler-%d").build());

    private static final Supplier<?> INIT = Suppliers
            .memoize(() -> SCHEDULER.scheduleWithFixedDelay(() -> {
                // try - cache 保护一下
                try {
                    GcUtil.doBatchFinalize();
                } catch (Throwable t) {
                    logger.warn("GcUtil doBatchFinalize exception", t);
                }
            }, 1, 1, TimeUnit.SECONDS));

    @VisibleForTesting
    static void addFinalizeListener(Runnable finalizeListener) {
        FINALIZE_LISTENER_FOR_UT.add(finalizeListener);
    }

    public static void registerCleanUp(Object o, Runnable cleanUp) {
        INIT.get();
        PhantomReference<?> phantomReference = new PhantomReference<>(o, REFERENCE_QUEUE);
        FINALIZER_MAP.put(phantomReference, cleanUp);
        // 非常极端的情况下这里有可能触发死锁，就不在这里清理资源了，依赖定时任务清理 FINALIZER_MAP
        // 这个 case 比较难复现
        //        doBatchFinalize();
    }

    @VisibleForTesting
    static int getFinalizerMapSize() {
        return FINALIZER_MAP.asMap().size();
    }

    @VisibleForTesting
    static void doBatchFinalize() {
        Reference<?> ref = REFERENCE_QUEUE.poll();
        while (ref != null) {
            //noinspection SuspiciousMethodCalls
            Collection<Runnable> finalizers = FINALIZER_MAP.removeAll(ref);
            if (finalizers != null) {
                FINALIZE_LISTENER_FOR_UT.forEach(r -> {
                    try {
                        r.run();
                    } catch (Throwable t) {
                        // ignore
                    }
                });
                finalizers.forEach(finalizer -> {
                    try {
                        finalizer.run();
                    } catch (Throwable t) {
                        logger.warn("config adapter auto close [{}] fail!", finalizer, t);
                    }
                });
            }
            ref = REFERENCE_QUEUE.poll();
        }
    }
}
