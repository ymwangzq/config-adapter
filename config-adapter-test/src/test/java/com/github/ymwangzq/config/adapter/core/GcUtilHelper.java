package com.github.ymwangzq.config.adapter.core;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.github.ymwangzq.config.adapter.facade.AdaptiveCleanUpProxy;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;

/**
 * @author myco
 * Created on 2020-01-02
 */
public class GcUtilHelper {
    public static void addFinalizeListener(Runnable finalizeListener) {
        GcUtil.addFinalizeListener(finalizeListener);
    }

    public static void doBatchFinalize() {
        GcUtil.doBatchFinalize();
    }

    public static int getFinalizerMapSize() {
        return GcUtil.getFinalizerMapSize();
    }

    // 这是真实的资源对象
    static class RealResource {
        // 这里搞一个 cleanup 方法被调用的计数器
        private static final AtomicInteger CLEAN_UP_COUNTER = new AtomicInteger(0);

        private final String configValue;

        public RealResource(String configValue) {
            this.configValue = configValue;
        }

        public String getConfigValue() {
            return configValue;
        }

        // 每当 cleanup 方法被调用的时候，就给计数器加1
        void cleanUp() {
            CLEAN_UP_COUNTER.incrementAndGet();
        }

        // 查看 cleanup 方法调用计数
        static int getCleanUpCounter() {
            return CLEAN_UP_COUNTER.get();
        }
    }

    // 壳对象
    static class ResourceProxy extends RealResource implements AdaptiveCleanUpProxy<RealResource> {
        private final RealResource delegate;

        public ResourceProxy(RealResource delegate) {
            super(delegate.getConfigValue());
            this.delegate = delegate;
        }

        @Override
        public RealResource getDelegate() {
            return delegate;
        }

        @Override
        public String getConfigValue() {
            return getDelegate().getConfigValue();
        }

        @Override
        public void cleanUp() {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void testAdaptiveCleanUp() {
        AtomicReference<String> configValue = new AtomicReference<>();
        configValue.set("v1");

        ConfigAdapter<String> dynamicConfigValue = SupplierConfigAdapter.createConfigAdapter(configValue::get);

        ConfigAdapter<ResourceProxy> configAdapter =
                ConfigAdapters.createAdaptiveCleanUp(dynamicConfigValue,
                        () -> new ResourceProxy(new RealResource(dynamicConfigValue.get())),
                        resource -> Optional.ofNullable(resource).ifPresent(RealResource::cleanUp));
        RealResource resource = configAdapter.get();
        // 验证是 v1
        Assertions.assertEquals("v1", resource.getConfigValue());
        Assertions.assertEquals(0, RealResource.getCleanUpCounter());
        // 配置更新成 v2
        configValue.set("v2");
        // 这里等 2s 是因为 SupplierConfigAdapter 内部 1s 才检查一次更新，等 2s 为了保证
        sleepUninterruptibly(2, SECONDS);
        // 验证是 v2
        Assertions.assertEquals("v2", configAdapter.get().getConfigValue());
        System.gc();
        // 这里等 2s 是因为 GcUtil 里的清理线程没 1s 执行一次
        sleepUninterruptibly(2, SECONDS);
        // 这时 cleanup 方法没有被执行
        Assertions.assertEquals(0, RealResource.getCleanUpCounter());
        // 把 resource 引用释放掉，让壳对象可以被 gc
        resource = null;
        System.gc();
        // 这里等 2s 是因为 GcUtil 里的清理线程没 1s 执行一次
        sleepUninterruptibly(2, SECONDS);
        // 这时 cleanup 方法被执行到了
        Assertions.assertEquals(1, RealResource.getCleanUpCounter());
    }
}
