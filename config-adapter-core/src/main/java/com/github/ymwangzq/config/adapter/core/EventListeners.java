package com.github.ymwangzq.config.adapter.core;

import static com.github.ymwangzq.config.adapter.core.BaseEvent.EventStatImpl.EVENT_STAT_FETCH_FAIL;

import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import com.github.ymwangzq.config.adapter.facade.StatefulEvent;
import com.google.common.base.MoreObjects;
import com.github.ymwangzq.config.adapter.facade.EventDispatcher;
import com.github.ymwangzq.config.adapter.facade.EventListener;

/**
 * @author myco
 * Created on 2019-10-25
 */
public class EventListeners {

    private EventListeners() {
    }

    private static final class BaseEventListener implements EventListener {

        private final Predicate<StatefulEvent> condition;
        private final Consumer<StatefulEvent> onEvent;
        private final String traceInfo;

        private BaseEventListener(@Nonnull Predicate<StatefulEvent> condition,
                @Nonnull Consumer<StatefulEvent> onEvent, String traceInfo) {
            this.condition = condition;
            this.onEvent = onEvent;
            this.traceInfo = MoreObjects.toStringHelper(this)
                    .add("condition", condition)
                    .add("onEvent", onEvent)
                    .add("traceInfo", traceInfo)
                    .toString();
        }

        private BaseEventListener(@Nonnull Predicate<StatefulEvent> condition,
                @Nonnull Consumer<StatefulEvent> onEvent) {
            this.condition = condition;
            this.onEvent = onEvent;
            this.traceInfo = MoreObjects.toStringHelper(this)
                    .add("condition", condition)
                    .add("onEvent", onEvent)
                    .toString();
        }

        @Override
        public String toString() {
            return traceInfo;
        }

        @Override
        public void processEvent(@Nonnull StatefulEvent event) {
            if (condition.test(event)) {
                onEvent.accept(event);
            }
        }
    }

    /**
     * @param callback 下游都通知完了之后的 callback
     */
    @Nonnull
    public static EventListener propagateEventListener(@Nonnull EventDispatcher eventDispatcher,
            @Nonnull Consumer<StatefulEvent> callback) {
        return new BaseEventListener(e -> true, e -> {
            // 新建一个 StatefulEvent 是为了阻止上游或者兄弟节点的失败传递到当前节点下游
            StatefulEvent subStreamEvent = BaseEvent.createSubEvent(e);
            eventDispatcher.dispatchEvent(subStreamEvent);
            callback.accept(subStreamEvent);
            // 下游的失败传递回上游
            e.getEventStat().addStats(subStreamEvent.getEventStat().getStats());
        });
    }

    /**
     * @param onUpdate 严禁在 onUpdate 中放可能无限阻塞的操作，必须要在有限时间内返回或者抛异常，几秒钟已经顶天了
     */
    @Nonnull
    public static EventListener updateEventListener(@Nonnull Runnable onUpdate) {
        return new BaseEventListener(e -> StatefulEvent.EventType.UPDATE.equals(e.getEventType()), e -> onUpdate.run());
    }

    @Nonnull
    static EventListener updateEventListener(@Nonnull Runnable onUpdate, @Nonnull String traceInfo) {
        return new BaseEventListener(e -> StatefulEvent.EventType.UPDATE.equals(e.getEventType()), e -> {
            try {
                onUpdate.run();
            } catch (Throwable t) {
                e.getEventStat().addStats(EVENT_STAT_FETCH_FAIL);
                throw t;
            }
        }, traceInfo);
    }

    /**
     * @param onFetchFail 严禁在 onFetchFail 中放可能无限阻塞的操作，必须要在有限时间内返回或者抛异常，几秒钟已经顶天了
     */
    @Nonnull
    public static EventListener fetchFailEventListener(@Nonnull Runnable onFetchFail) {
        return new BaseEventListener(e -> StatefulEvent.EventType.FETCH_FAIL.equals(e.getEventType()), e -> onFetchFail.run());
    }

    /**
     * @param onCleanUpFail 严禁在 onCleanUpFail 中放可能无限阻塞的操作，必须要在有限时间内返回或者抛异常，几秒钟已经顶天了
     */
    @Nonnull
    public static EventListener cleanUpFailEventListener(@Nonnull Runnable onCleanUpFail) {
        return new BaseEventListener(e -> StatefulEvent.EventType.CLEAN_UP_FAIL.equals(e.getEventType()), e -> onCleanUpFail.run());
    }
}
