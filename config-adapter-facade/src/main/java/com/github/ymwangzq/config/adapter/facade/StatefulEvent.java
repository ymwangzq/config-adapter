package com.github.ymwangzq.config.adapter.facade;

import java.util.List;

import javax.annotation.Nonnull;

/**
 * 强调！
 * <p>
 * event 本身不传输数据, 只作为事件通知
 *
 * @author myco
 * Created on 2019-10-11
 */
public interface StatefulEvent {

    interface EventStat {
        boolean isSuccess();

        void addStats(long stats);

        boolean hasStats(long stats);

        long getStats();
    }

    enum EventType {
        UPDATE,
        FETCH_FAIL,
        CLEAN_UP_FAIL
    }

    @Nonnull
    EventType getEventType();

    @Nonnull
    EventStat getEventStat();

    void addTraceInfo(String eventListenerToString);

    List<String> getTraceInfo();
}
