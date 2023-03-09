package com.github.ymwangzq.config.adapter.core;

import java.lang.reflect.Method;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ymwangzq.config.adapter.facade.EventDispatcher;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * 为了跑单元测试的时候，可以手动触发更新事件，简单反射搞了
 *
 * @author myco
 * Created on 2019-11-11
 */
public abstract class RootConfigAdapterHelper {

    private static final Logger logger = LoggerFactory.getLogger(RootConfigAdapterHelper.class);

    // 如果用户传入的 mapper 有无限阻塞逻辑，最多缓存多少新的 event；配置更新的场景应该只缓存最新一次就够了
    private static final int MAX_QUEUED_EVENT_NUM = 1;

    private static final Method REGISTER_ROOT_EVENT_SOURCE;

    static {
        Method registerRootEventSourceMethod;
        try {
            registerRootEventSourceMethod = RootConfigAdapterHelper.class.getClassLoader()
                    .loadClass("com.github.ymwangzq.config.adapter.core.ConfigAdapterTestHelper")
                    .getDeclaredMethod("registerEventDispatcher", EventDispatcher.class);
        } catch (Throwable t) {
            registerRootEventSourceMethod = null;
        }
        REGISTER_ROOT_EVENT_SOURCE = registerRootEventSourceMethod;
    }

    protected static void registerRootEventDispatcher(EventDispatcher eventDispatcher) {
        if (REGISTER_ROOT_EVENT_SOURCE != null) {
            try {
                REGISTER_ROOT_EVENT_SOURCE.invoke(null, eventDispatcher);
            } catch (Throwable t) {
                logger.debug("注册 RootEventDispatcher 失败", t);
            }
        }
    }

    /**
     * 单线程分发事件，不能增加线程，可能会导致死锁
     */
    protected static SingleThreadConfigDispatchExecutor createEventDispatchExecutor(@Nonnull String threadNameFormat) {
        Preconditions.checkArgument(StringUtils.isNotBlank(threadNameFormat));
        return new SingleThreadConfigDispatchExecutorImpl(new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(threadNameFormat)
                .build(), 1, TimeUnit.SECONDS);
    }

    private static volatile boolean TEST_INTERRUPT_BUG = false;

    @VisibleForTesting
    static void setTestInterruptBug(boolean testInterruptBug) {
        TEST_INTERRUPT_BUG = testInterruptBug;
    }

    private static final AtomicLong DISPATCH_COUNTER = new AtomicLong(0);

    protected static Future<?> dispatchUpdateEvent(EventDispatcher eventDispatcher,
            SingleThreadConfigDispatchExecutor eventDispatchExecutor) {
        long dispatchId = DISPATCH_COUNTER.getAndIncrement();
        logger.trace("dispatch task submit [{}], ts: [{}]", dispatchId, System.currentTimeMillis());
        return eventDispatchExecutor.interruptCurrentAndSubmit(() -> {
            logger.trace("dispatch task start [{}], ts: [{}]", dispatchId, System.currentTimeMillis());
            if (TEST_INTERRUPT_BUG) {
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
            try {
                eventDispatcher.dispatchEvent(BaseEvent.newUpdateEvent());
            } finally {
                logger.trace("dispatch task done [{}], ts: [{}]", dispatchId, System.currentTimeMillis());
            }
            return null;
        });
    }

    protected RootConfigAdapterHelper() {
        throw new UnsupportedOperationException();
    }
}
