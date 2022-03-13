package com.github.ymwangzq.config.adapter.core;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.Nonnull;

import com.github.ymwangzq.config.adapter.facade.StatefulEvent;

/**
 * @author myco
 * Created on 2019-10-23
 */
public class BaseEvent implements StatefulEvent {

    private final EventType eventType;
    private final List<String> eventListenerTrace = new LinkedList<>();
    private EventStat eventStat = new EventStatImpl();

    static class EventStatImpl implements EventStat {

        static final long EVENT_STAT_FETCH_FAIL = 1;
        static final long EVENT_STAT_CLEANUP_FAIL = 1 << 1;

        private long stat = 0;

        @Override
        public boolean isSuccess() {
            return stat == 0;
        }

        @Override
        public void addStats(long stats) {
            stat |= stats;
        }

        @Override
        public boolean hasStats(long stats) {
            return (stat & stats) == stats;
        }

        @Override
        public long getStats() {
            return stat;
        }
    }


    public static StatefulEvent createSubEvent(@Nonnull StatefulEvent event) {
        switch (event.getEventType()) {
            case UPDATE:
                return newUpdateEvent();
            case FETCH_FAIL:
                return newFetchFailEvent();
            case CLEAN_UP_FAIL:
                return newCleanUpFailEvent();
            default:
                throw new UnsupportedOperationException("未知的 eventType: " + event.getEventType());
        }
    }

    public static StatefulEvent newUpdateEvent() {
        return new BaseEvent(EventType.UPDATE);
    }

    public static StatefulEvent newFetchFailEvent() {
        return new BaseEvent(EventType.FETCH_FAIL);
    }

    public static StatefulEvent newCleanUpFailEvent() {
        return new BaseEvent(EventType.CLEAN_UP_FAIL);
    }

    private BaseEvent(@Nonnull EventType eventType) {
        this.eventType = eventType;
    }

    @Nonnull
    @Override
    public EventType getEventType() {
        return eventType;
    }

    @Nonnull
    @Override
    public EventStat getEventStat() {
        return eventStat;
    }

    @Override
    public void addTraceInfo(String eventListenerToString) {
        eventListenerTrace.add(eventListenerToString);
    }

    @Override
    public List<String> getTraceInfo() {
        return Collections.unmodifiableList(eventListenerTrace);
    }
}
