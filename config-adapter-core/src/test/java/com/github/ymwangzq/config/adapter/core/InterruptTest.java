package com.github.ymwangzq.config.adapter.core;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.Uninterruptibles;
import com.github.ymwangzq.config.adapter.core.UpdateWithRetryMapper.RetryForever;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;

/**
 * @author myco
 * Created on 2021-05-28
 */
public class InterruptTest {

    /**
     * 一旦 interrupt 被外部 mapper 吞掉的情况，尽可能保证在后续 Retry 的过程中有机会再次被 interrupt。
     * <p>
     * 这个 case 暂时跑不过，需要修复
     */
    @Test
    void testInterrupt() {
        AtomicInteger configSource = new AtomicInteger(0);
        ConfigAdapter<Integer> configAdapter = SupplierConfigAdapter.createConfigAdapter(configSource::get);
        ConfigAdapter<String> configAdapter1 = configAdapter
                // 用 retryForever map 一次
                .map(UpdateWithRetryMapper.wrap(i -> {
                    System.out.println("retry with int: " + i + " in thread: " + Thread.currentThread().getName());
                    if (i == 1) {
                        // 模拟一个错误的配置，需要花较长时间，同时也会吞掉 interrupt 标记
                        Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
                        // 清理掉 interrupt 状态，这里是导致不能更新到后续正确配置的核心原因
                        //noinspection ResultOfMethodCallIgnored
                        Thread.interrupted();
                        throw new RuntimeException("bbb");
                    }
                    return "string: " + i;
                }, new RetryForever(2, 10, 1000, null)));

        System.out.println(configAdapter1.get());
        // 首先更新一个错误配置
        configSource.set(1);
        Uninterruptibles.sleepUninterruptibly(2100, TimeUnit.MILLISECONDS);
        // 再更新一个正确配置
        configSource.set(2);
        Uninterruptibles.sleepUninterruptibly(2100, TimeUnit.MILLISECONDS);

        // 这里看 configAdapter1 的值能否更新到最新的正确值
        for (int i = 0; i < 20; i++) {
            configSource.incrementAndGet();
            System.out.println("configSource: " + configSource.get() + ", configValue: " + configAdapter1.get());
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        for (int i = 0; i < 3; i++) {
            System.out.println("configSource: " + configSource.get() + ", configValue: " + configAdapter1.get());
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        Assertions.assertEquals("string: " + configSource.get(), configAdapter1.get());
    }

    @Test
    void testInterrupt2() {
        System.gc();
        RootConfigAdapterHelper.setTestInterruptBug(true);
        try {
            AtomicInteger configSource = new AtomicInteger(0);
            ConfigAdapter<Integer> configAdapter = SupplierConfigAdapter.createConfigAdapter(configSource::get);
            ConfigAdapter<String> configAdapter1 = configAdapter
                    // 用 retryForever map 一次
                    .map(UpdateWithRetryMapper.wrap(i -> {
                        System.out.println("retry with int: " + i + " in thread: " + Thread.currentThread().getName());
                        if (i > 1 && i <= 10) {
                            // 模拟一个错误的配置，抛出异常
                            throw new RuntimeException("bbb");
                        }
                        return "string: " + i;
                    }, new RetryForever(2, 10, 1000, null)));

            System.out.println(configAdapter1.get());

            configSource.set(1);
            Uninterruptibles.sleepUninterruptibly(3000, TimeUnit.MILLISECONDS);
            System.out.println("configSource: " + configSource.get() + ", configValue: " + configAdapter1.get());
            Assertions.assertEquals("string: " + configSource.get(), configAdapter1.get());

            // 这里看 configAdapter1 的值能否更新到最新的正确值
            for (int i = 0; i < 20; i++) {
                configSource.incrementAndGet();
                System.out.println("configSource: " + configSource.get() + ", configValue: " + configAdapter1.get());
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
            for (int i = 0; i < 10 && !StringUtils.equals("string: " + configSource.get(), configAdapter1.get()); i++) {
                System.out.println("configSource: " + configSource.get() + ", configValue: " + configAdapter1.get());
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
            Assertions.assertEquals("string: " + configSource.get(), configAdapter1.get());
        } finally {
            RootConfigAdapterHelper.setTestInterruptBug(false);
        }
    }
}
