package com.github.ymwangzq.config.adapter.core;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;

/**
 * @author myco
 * Created on 2020-08-10
 */
class BaseConfigAdapterTest {

    @Test
    void testDeadLock1() {

        ExecutorService executorService = Executors
                .newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("deadLockExecutor-%d").build());
        try {
            for (int i = 0; i < 1000; i++) {
                AtomicReference<String> data1 = new AtomicReference<>("d1-v1");
                BaseEventSource eventSource1 = new BaseEventSource();
                ConfigAdapter<String> configAdapter1 = ConfigAdapters.createMapOnEachUpdate(eventSource1, () -> {
                    boolean flag = true;
                    if (flag) {
                        System.out.println("getter throws");
                        throw new RuntimeException();
                    }
                    return data1.get();
                }, s -> { });

                ConfigAdapter<String> configAdapter2 = configAdapter1.map(s -> {
                    return s;
                });

                Future<?> future1 = executorService.submit(() -> {
                    Thread.currentThread().setName("getter");
                    try {
                        configAdapter2.get();
                    } catch (Throwable t) {
//                        t.printStackTrace();
                    }
                });

                Future<?> future2 = executorService.submit(() -> {
                    Thread.currentThread().setName("updater");
                    eventSource1.dispatchEvent(BaseEvent.newUpdateEvent());
                });

                future1.get(10, SECONDS);
                future2.get(10, SECONDS);
            }
        } catch (Throwable t) {
//            t.printStackTrace();
            while (true) {
                System.out.println("！！！应该是出现死锁了，jstack 一个看看！！！");
                Uninterruptibles.sleepUninterruptibly(1, SECONDS);
            }
        } finally {
            executorService.shutdown();
        }
    }
}