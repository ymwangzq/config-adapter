package com.github.ymwangzq.config.adapter.core;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.Uninterruptibles;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;

/**
 * @author myco
 * Created on 2021-11-22
 */
class ConfigAdapterInitializerTest {

    @Test
    void testConfigAdapterInitializer() {
        System.gc();
        AtomicBoolean initSuccess = new AtomicBoolean(false);
        String v1 = RandomStringUtils.randomAlphabetic(10);
        AtomicReference<String> dataSource = new AtomicReference<>(v1);
        AtomicLong fetcherCounter = new AtomicLong(0);
        Supplier<String> fetcher = () -> {
            fetcherCounter.incrementAndGet();
            if (initSuccess.get()) {
                return dataSource.get();
            } else {
                throw new RuntimeException("fetcher failed!");
            }
        };
        int initQueueSize = ConfigAdapterInitializer.initQueueSize();
        BaseEventSource eventSource = new BaseEventSource();
        ConfigAdapter<String> configAdapter =
                ConfigAdapters.createMapOnEachUpdate(eventSource, fetcher, s -> {});
        Assertions.assertEquals(initQueueSize + 1, ConfigAdapterInitializer.initQueueSize());

        Assertions.assertEquals(0L, fetcherCounter.get());
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        Assertions.assertEquals(0L, fetcherCounter.get());

        eventSource.dispatchEvent(BaseEvent.newUpdateEvent());
        Assertions.assertEquals(0L, fetcherCounter.get());

        Assertions.assertThrows(RuntimeException.class, configAdapter::get);
        Assertions.assertEquals(1L, fetcherCounter.get());

        Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(2, fetcherCounter.get());

        Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(3, fetcherCounter.get());
        Assertions.assertEquals(initQueueSize + 1, ConfigAdapterInitializer.initQueueSize());

        initSuccess.set(true);
        Assertions.assertEquals(v1, configAdapter.get());
        Assertions.assertEquals(4, fetcherCounter.get());

        Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(4, fetcherCounter.get());
        Assertions.assertEquals(initQueueSize, ConfigAdapterInitializer.initQueueSize());

        eventSource.dispatchEvent(BaseEvent.newUpdateEvent());
        Assertions.assertEquals(5, fetcherCounter.get());
    }
}