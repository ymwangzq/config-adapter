package com.github.ymwangzq.config.adapter.core;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.github.ymwangzq.config.adapter.facade.EventListener;
import com.github.ymwangzq.config.adapter.facade.EventSource;

/**
 * @author myco
 * Created on 2019-10-23
 */
public final class EventSourceUtil {

    private EventSourceUtil() {
    }

    private static class HoldRefEventSource extends DelegatingEventSource {

        private final Object[] refs;

        public HoldRefEventSource(EventSource delegate, Object... refs) {
            super(delegate);
            this.refs = refs;
        }
    }

    private static class RemoveEventListenerTask1 implements Runnable {

        private final Stream<EventSource> eventSourceStream;
        private final EventListener eventListener;

        private RemoveEventListenerTask1(
                @Nonnull Stream<EventSource> eventSourceStream, @Nonnull EventListener eventListener) {
            this.eventSourceStream = eventSourceStream;
            this.eventListener = eventListener;
        }

        @Override
        public void run() {
            eventSourceStream.forEach(eventSource -> eventSource.removeEventListener(eventListener));
        }
    }

    private static class RemoveEventListenerTask2 implements Runnable {

        private final Iterable<? extends EventSource> eventSourceStream;
        private final EventListener eventListener;

        private RemoveEventListenerTask2(
                @Nonnull Iterable<? extends EventSource> eventSourceStream, @Nonnull EventListener eventListener) {
            this.eventSourceStream = eventSourceStream;
            this.eventListener = eventListener;
        }

        @Override
        public void run() {
            eventSourceStream.forEach(eventSource -> eventSource.removeEventListener(eventListener));
        }
    }

    /**
     * 返回的 EventSource 是一个壳对象
     * 需要保证在返回的 EventSource 壳对象中持有上游 eventSource 的引用；避免上游被 gc 掉以后，通知断链
     * 还需要在返回的 EventSource 壳对象被 gc 时，从上游 eventSource 里删掉对应的 listener，让 core 对象能够被 gc 掉
     */
    @Nonnull
    public static EventSource anyOf(@Nonnull Iterable<? extends EventSource> eventSources) {
        BaseEventSource core = new BaseEventSource();
        Preconditions.checkNotNull(eventSources, "eventSources 不能为 null");
        EventListener eventListener = core::dispatchEvent;
        eventSources.forEach(eventSource -> eventSource.addEventListener(eventListener));
        EventSource returnObj = new HoldRefEventSource(core, eventSources);
        GcUtil.registerCleanUp(returnObj, new RemoveEventListenerTask2(eventSources, eventListener));
        return returnObj;
    }

    @Nonnull
    public static EventSource anyOf(EventSource... eventSources) {
        BaseEventSource core = new BaseEventSource();
        Preconditions.checkNotNull(eventSources, "eventSources 不能为 null");
        EventListener eventListener = core::dispatchEvent;
        Stream.of(eventSources).forEach(eventSource -> eventSource.addEventListener(eventListener));
        EventSource returnObj = new HoldRefEventSource(core, (Object) eventSources);
        GcUtil.registerCleanUp(returnObj, new RemoveEventListenerTask1(Stream.of(eventSources), eventListener));
        return returnObj;
    }
}
