package com.github.ymwangzq.config.adapter.core;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.github.ymwangzq.config.adapter.core.UpdateWithRetryMapper.RetryForever;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;

/**
 * @author myco
 * Created on 2019-12-10
 */
class UpdateWithRetryMapperTest {

    @Test
    void testPowOverFlow() {
        double result = Math.pow(100, 10000);
        System.out.println(result);
        long b = ((long) (2 * (result * result)));
        System.out.println(b);
        System.out.println(Long.MAX_VALUE == b);
        long c = ((long) (-2 * (result * result)));
        System.out.println(c);
        System.out.println(Long.MIN_VALUE == c);
        System.out.println(Double.MAX_VALUE * 2);
        System.out.println(Math.min(Double.MAX_VALUE * 2, 10.0));
    }

    /**
     * 测试多级使用 mapWithRetry 的 ConfigAdapter，在连续下发新配置时最终可以拿到最新的配置
     */
    @Test
    void testMultiMapWithRetryWithInterrupt() {

        // 构造一个不变的 ConfigAdapter，用于做 merge
        ConfigAdapter<String> ca0 = SupplierConfigAdapter.createConfigAdapter(() -> "");

        String A = "A";
        String B = "B";
        String C = "C";
        String D = "D";
        String E = "E";
        Set<String> brokenInstances = Sets.newConcurrentHashSet();
        AtomicReference<String> configSource = new AtomicReference<>();
        ConfigAdapter<String> ca1 = SupplierConfigAdapter.createConfigAdapter(configSource::get);
        ConfigAdapter<List<String>> ca2 = ca1.map(UpdateWithRetryMapper.wrap(from -> {
            List<String> instances = Arrays.asList(StringUtils.split(from, ","));
            instances.forEach(instance -> {
                // 模拟每次配置变更检查是否有宕掉的实例
                if (brokenInstances.contains(instance)) {
                    throw new RuntimeException("broken instance: " + instance);
                }
            });
            return instances;
        }, new RetryForever(2, 1000, 5000, null)));

        ConfigAdapter<List<String>> ca3 =
                ConfigAdapters.mergeTwoConfigAdapters(ca0, ca2, (str, list) -> list, c -> { });

        ConfigAdapter<Set<String>> ca4 = ca3.map(UpdateWithRetryMapper.wrap(instanceList -> {
            instanceList.forEach(instance -> {
                // 再次检查是否有宕掉的实例
                if (brokenInstances.contains(instance)) {
                    throw new RuntimeException("broken instance: " + instance);
                }
            });
            return new HashSet<>(instanceList);
        }, new RetryForever(2, 1000, 5000, null)));

        // 初始状态，三个实例
        configSource.set(StringUtils.joinWith(",", A, B, C));
        // 确认配置正确
        Assertions.assertTrue(Sets.difference(ca4.get(), ImmutableSet.of(A, B, C)).isEmpty());
        // 模拟 A B 宕机
        brokenInstances.add(A);
        brokenInstances.add(B);
        // 模拟配置切换 A -> D
        configSource.set(StringUtils.joinWith(",", D, B, C));
        // 等待配置更新
        Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);
        // 验证配置并没有更新，因为 B 仍然是挂的
        Assertions.assertTrue(Sets.difference(ca4.get(), ImmutableSet.of(A, B, C)).isEmpty());
        // 稍等一段时间
        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
        // 模拟配置切换 B -> E
        configSource.set(StringUtils.joinWith(",", D, E, C));
        // 等待配置更新
        Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);
        // 验证配置已经更新
        int i = 0;
        while (!Sets.difference(ca4.get(), ImmutableSet.of(D, E, C)).isEmpty() && i++ < 20) {
            System.out.println(ca4.get());
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        System.out.println(ca4.get());
        Assertions.assertTrue(Sets.difference(ca4.get(), ImmutableSet.of(D, E, C)).isEmpty());
    }

    /**
     * 测试从一个 ConfigAdapter mapWithRetry 出多个 ConfigAdapter 的情况下，在连续下发新配置时，可以拿到最新的配置
     */
    @Test
    void testMultiMapWithRetryWithInterrupt2() {

        String A = "A";
        String B = "B";
        String C = "C";
        String D = "D";
        String E = "E";

        Set<String> brokenInstances = Sets.newConcurrentHashSet();

        AtomicReference<String> configSource = new AtomicReference<>();
        ConfigAdapter<String> ca1 = SupplierConfigAdapter.createConfigAdapter(configSource::get);

        ConfigAdapter<Set<String>> ca2 = ca1.map(UpdateWithRetryMapper.wrap(from -> {
            List<String> instances = Arrays.asList(StringUtils.split(from, ","));
            instances.forEach(instance -> {
                // 模拟每次配置变更检查是否有宕掉的实例
                if (brokenInstances.contains(instance)) {
                    throw new RuntimeException("broken instance: " + instance);
                }
            });
            return new HashSet<>(instances);
        }, new RetryForever(2, 1000, 5000, null)));

        ConfigAdapter<Set<String>> ca3 = ca1.map(UpdateWithRetryMapper.wrap(from -> {
            List<String> instances = Arrays.asList(StringUtils.split(from, ","));
            instances.forEach(instance -> {
                // 模拟每次配置变更检查是否有宕掉的实例
                if (brokenInstances.contains(instance)) {
                    throw new RuntimeException("broken instance: " + instance);
                }
            });
            return new HashSet<>(instances);
        }, new RetryForever(2, 1000, 5000, null)));

        // 初始状态，三个实例
        configSource.set(StringUtils.joinWith(",", A, B, C));
        // 确认配置正确
        Assertions.assertTrue(Sets.difference(ca2.get(), ImmutableSet.of(A, B, C)).isEmpty());
        Assertions.assertTrue(Sets.difference(ca3.get(), ImmutableSet.of(A, B, C)).isEmpty());
        // 模拟 A B 宕机
        brokenInstances.add(A);
        brokenInstances.add(B);
        // 模拟配置切换 A -> D
        configSource.set(StringUtils.joinWith(",", D, B, C));
        // 等待配置更新
        Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);
        // 验证配置并没有更新，因为 B 仍然是挂的
        Assertions.assertTrue(Sets.difference(ca2.get(), ImmutableSet.of(A, B, C)).isEmpty());
        Assertions.assertTrue(Sets.difference(ca3.get(), ImmutableSet.of(A, B, C)).isEmpty());
        // 稍等一段时间
        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
        // 模拟配置切换 B -> E
        configSource.set(StringUtils.joinWith(",", D, E, C));
        // 等待配置更新
        Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);
        // 验证配置已经更新
        int i = 0;
        while (!Sets.difference(ca2.get(), ImmutableSet.of(D, E, C)).isEmpty() && i++ < 20) {
            System.out.println("ca2: " + ca2.get());
            System.out.println("ca3: " + ca3.get());
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        System.out.println("ca2: " + ca2.get());
        System.out.println("ca3: " + ca3.get());
        Assertions.assertTrue(Sets.difference(ca2.get(), ImmutableSet.of(D, E, C)).isEmpty());
        Assertions.assertTrue(Sets.difference(ca3.get(), ImmutableSet.of(D, E, C)).isEmpty());
    }
}