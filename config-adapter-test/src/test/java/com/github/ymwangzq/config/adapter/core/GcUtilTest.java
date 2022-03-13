package com.github.ymwangzq.config.adapter.core;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapterMapper;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * @author myco
 * Created on 2021-11-05
 */
class GcUtilTest {

    private static class SomeResource implements AutoCloseable {
        private static final AtomicLong instanceCounter = new AtomicLong(0);

        private final long data;

        public SomeResource(long data) {
            this.data = data;
            instanceCounter.incrementAndGet();
        }

        public long getData() {
            return data;
        }

        @Override
        public void close() {
            instanceCounter.decrementAndGet();
        }

        static long getInstanceNumber() {
            return instanceCounter.get();
        }
    }

    /**
     * 曾经由于在 {@link GcUtil#registerCleanUp(java.lang.Object, java.lang.Runnable)} 中调用 {@link GcUtil#doBatchFinalize()}
     * 导致频繁变更时出现死锁，这个 case 尝试模拟这种情况
     */
    @Test
    void testDeadLock() {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        AtomicBoolean stopCreate = new AtomicBoolean(false);
        AtomicBoolean stopUpdate = new AtomicBoolean(false);
        //        ConfigAdapterTestHelper.enableAutoRefresh(Duration.ofMillis(100));
        try {
            AtomicLong configSource = new AtomicLong(0);
            ConcurrentLinkedQueue<ConfigAdapter<SomeResource>> queue = new ConcurrentLinkedQueue<>();
            executorService.submit(() -> {
                while (!stopCreate.get()) {
                    ConfigAdapter<SomeResource> config = newActivatedConfigAdapter(configSource);
                    if (ThreadLocalRandom.current().nextBoolean()) {
                        queue.add(config);
                    }
                    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.NANOSECONDS);
                }
            });
            executorService.submit(() -> {
                while (!stopUpdate.get()) {
                    configSource.incrementAndGet();
                }
            });
            for (int i = 0; i < 5; i++) {
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                System.out.println("SomeResource.getInstanceNumber(): " + SomeResource.getInstanceNumber());
                System.gc();
                queue.clear();
            }
            Assertions.assertTrue(SomeResource.getInstanceNumber() > 0);
            stopCreate.set(true);
            for (int i = 0; i < 20; i++) {
                System.gc();
                queue.clear();
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                System.out.println("SomeResource.getInstanceNumber(): " + SomeResource.getInstanceNumber());
                if (SomeResource.getInstanceNumber() == 0) {
                    break;
                }
            }
            Assertions.assertEquals(0, SomeResource.getInstanceNumber());
        } finally {
            ConfigAdapterTestHelper.disableAutoRefresh();
            stopCreate.set(true);
            stopUpdate.set(true);
            executorService.shutdown();
        }
    }

    private ConfigAdapter<SomeResource> newActivatedConfigAdapter(AtomicLong configSource) {
        ConfigAdapter<SomeResource> config = SupplierConfigAdapter.createConfigAdapter(configSource::get)
                .map(new ConfigAdapterMapper<Long, SomeResource>() {
                    @Override
                    public SomeResource map(@Nullable Long l) {
                        return new SomeResource(Optional.ofNullable(l).orElse(0L));
                    }

                    @Override
                    public void cleanUp(@Nullable SomeResource someResource) {
                        if (someResource != null) {
                            someResource.close();
                        }
                    }
                });
        config.get();
        return config;
    }
}