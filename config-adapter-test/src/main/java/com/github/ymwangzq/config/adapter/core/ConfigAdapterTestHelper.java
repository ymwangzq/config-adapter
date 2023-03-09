package com.github.ymwangzq.config.adapter.core;

import static java.time.temporal.ChronoUnit.NANOS;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.github.ymwangzq.config.adapter.facade.EventDispatcher;
import com.github.ymwangzq.config.adapter.facade.StatefulEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 外部有反射调用，本类不可轻易重构（改类名、方法名等）
 *
 * @author myco
 * Created on 2019-11-11
 */
@VisibleForTesting
public class ConfigAdapterTestHelper {

    private ConfigAdapterTestHelper() {
    }

    private static final Queue<EventDispatcher> EVENT_DISPATCHERS = Queues.newConcurrentLinkedQueue();

    static void registerEventDispatcher(EventDispatcher eventDispatcher) {
        EVENT_DISPATCHERS.offer(eventDispatcher);
    }

    /**
     * 单元测试中生效, 向所有的 RootConfigAdapter 发送一个事件
     */
    public static void dispatchEvent(StatefulEvent event) {
        EVENT_DISPATCHERS.forEach(eventDispatcher ->
                eventDispatcher.dispatchEvent(event));
    }

    private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("ConfigAdapterTestHelper-%d")
                    .build());

    private static final AtomicReference<ScheduledFuture<?>> REFRESH_FUTURE = new AtomicReference<>();

    public static void enableAutoRefresh(Duration refreshDuration) {
        ScheduledFuture<?> future = SCHEDULED_EXECUTOR_SERVICE.scheduleAtFixedRate(() ->
                        dispatchEvent(BaseEvent.newUpdateEvent()),
                refreshDuration.get(NANOS), refreshDuration.get(NANOS), TimeUnit.NANOSECONDS);
        if (!REFRESH_FUTURE.compareAndSet(null, future)) {
            future.cancel(false);
        }
    }

    public static void disableAutoRefresh() {
        ScheduledFuture<?> future = REFRESH_FUTURE.getAndSet(null);
        if (future != null) {
            future.cancel(false);
        }
    }
}
