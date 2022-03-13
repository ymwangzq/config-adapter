package com.github.ymwangzq.config.adapter.core;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ymwangzq.config.adapter.facade.StatefulEvent;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.github.ymwangzq.config.adapter.facade.EventDispatcher;
import com.github.ymwangzq.config.adapter.facade.EventListener;
import com.github.ymwangzq.config.adapter.facade.EventSource;


/**
 * @author myco
 * Created on 2019-10-11
 */
@ThreadSafe
public final class BaseEventSource implements EventSource, EventDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(BaseEventSource.class);

    private static final long PROCESS_DURATION_MS_WARNING_THRESHOLD = 5000L;

    private final ConcurrentLinkedQueue<EventListener> eventListenerQueue = new ConcurrentLinkedQueue<>();

    private final Object dispatchEventLock = new Object();

    private final ThreadLocal<Map<StatefulEvent.EventType, StatefulEvent>> processingEventTL =
            ThreadLocal.withInitial(HashMap::new);

    @Override
    public void dispatchEvent(@Nonnull StatefulEvent event) {
        Preconditions.checkNotNull(event);
        // 通常情况下 event source 都是由单线程触发, 这里加锁能保证多线程触发 event source 的时候也没有线程安全问题
        synchronized (dispatchEventLock) {
            StatefulEvent processingEvent = this.processingEventTL.get().get(event.getEventType());
            if (processingEvent != null) {
                logger.error("EventSource 搞出环了!!! eventTrace1: [{}], eventTrace2: [{}] stackTrace: ",
                        processingEvent.getTraceInfo(), event.getTraceInfo(), new RuntimeException());
                return;
            }
            this.processingEventTL.get().put(event.getEventType(), event);
            try {
                eventListenerQueue.forEach(eventListener -> {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    try {
                        event.addTraceInfo(eventListener.toString());
                        eventListener.processEvent(event);
                    } catch (Throwable t) {
                        // 这里会 catch 住所有异常，继续通知其他下游做更新，希望尽可能通知到更多的下游。
                        logger.warn("processEvent fail-safe: ", t);
                        if (Throwables.getRootCause(t) instanceof InterruptedException) {
                            // 如果当多个下游都使用 UpdateWithRetryMapper 时，新配置下发时触发的 interrupt 异常
                            // 也会被 catch 住，然后交给下游的 UpdateWithRetryMapper 处理，有很大概率再次陷入重试，导致新的配置无法下发。
                            // 所以这里判断下 root cause，如果是 InterruptedException 就重新标记回去
                            Thread.currentThread().interrupt();
                        }
                    } finally {
                        long processDuration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                        if (processDuration > PROCESS_DURATION_MS_WARNING_THRESHOLD) {
                            logger.trace("eventListener 中可能有阻塞操作, 耗时: {} ms, {}", processDuration, eventListener);
                        }
                    }
                });
            } finally {
                this.processingEventTL.remove();
            }
        }
    }

    @Override
    public void addEventListener(@Nonnull EventListener eventListener) {
        eventListenerQueue.offer(eventListener);
    }

    @Override
    public void removeEventListener(@Nonnull EventListener eventListener) {
        Preconditions.checkState(eventListenerQueue.remove(eventListener));
    }
}
