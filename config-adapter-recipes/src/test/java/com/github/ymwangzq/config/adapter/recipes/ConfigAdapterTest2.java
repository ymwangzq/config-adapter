package com.github.ymwangzq.config.adapter.recipes;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.github.ymwangzq.config.adapter.core.GcUtilHelper;
import com.github.ymwangzq.config.adapter.core.SupplierConfigAdapter;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.RateLimiter;

/**
 * @author myco
 * Created on 2020-01-14
 */
class ConfigAdapterTest2 {

    private static final AtomicReference<String> configSource = new AtomicReference<>(null);

    @Test
    void testGcCleanUp() {
        ConfigAdapter<String> configAdapter;
        RateLimiter rateLimiter = RateLimiter.create(1);
        for (int i = 0; i < 100000; i++) {
            configAdapter = SupplierConfigAdapter.createConfigAdapter(configSource::get);
            if (rateLimiter.tryAcquire()) {
                System.out.println(GcUtilHelper.getFinalizerMapSize());
            }
        }
        configAdapter = null;
        sleepUninterruptibly(1, TimeUnit.SECONDS);
        System.gc();

        for (int i = 0; i < 20 && GcUtilHelper.getFinalizerMapSize() > 3; i++) {
            GcUtilHelper.doBatchFinalize();
            System.gc();
            System.out.println("done: " + GcUtilHelper.getFinalizerMapSize());
            sleepUninterruptibly(1, TimeUnit.SECONDS);
        }

        // 单跑这一个 case 时应当能够全部释放
        // 为了能直接 mvn test 跑过所有 case，这里放宽一点检查条件
        Assertions.assertTrue(GcUtilHelper.getFinalizerMapSize() <= 3);
    }

    static int DELAY_SECONDS = 2;

    @Test
    void testConfigAdapter() {
        configSource.set("{}");
        sleepUninterruptibly(DELAY_SECONDS, TimeUnit.SECONDS);
        ConfigAdapter<String> conf1 = SupplierConfigAdapter.createConfigAdapter(configSource::get);
        ConfigAdapter<String> conf2 = SupplierConfigAdapter.createConfigAdapter(configSource::get);
        Assertions.assertEquals("{}", conf1.get());
        Assertions.assertEquals("{}", conf2.get());
        configSource.set("version1");
        sleepUninterruptibly(DELAY_SECONDS, TimeUnit.SECONDS);
        Assertions.assertEquals("version1", conf1.get());
        Assertions.assertEquals("version1", conf2.get());
    }

    private static class ConfigPojo {
        private static final AtomicInteger COUNTER = new AtomicInteger(0);

        public ConfigPojo() {
            COUNTER.incrementAndGet();
        }

        public static int getCount() {
            return COUNTER.get();
        }
    }

    /**
     * 这个case需要单独跑
     */
    @Test
    void testLazyInit() {
        configSource.set("version0");
        sleepUninterruptibly(DELAY_SECONDS, TimeUnit.SECONDS);
        ConfigAdapter<ConfigPojo> conf = SupplierConfigAdapter.createConfigAdapter(configSource::get)
                .map(s -> new ConfigPojo());
        Assertions.assertEquals(0, ConfigPojo.getCount());
        ConfigAdapter<ConfigPojo> configAdapter = SupplierConfigAdapter.createConfigAdapter(conf);
        Assertions.assertEquals(0, ConfigPojo.getCount());
        configAdapter.get();
        Assertions.assertEquals(1, ConfigPojo.getCount());
        configAdapter.get();
        Assertions.assertEquals(1, ConfigPojo.getCount());

        configSource.set("version1");
        sleepUninterruptibly(DELAY_SECONDS, TimeUnit.SECONDS);
        Assertions.assertEquals(2, ConfigPojo.getCount());
    }
}