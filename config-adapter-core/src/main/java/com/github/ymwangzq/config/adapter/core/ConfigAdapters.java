package com.github.ymwangzq.config.adapter.core;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.github.ymwangzq.config.adapter.facade.AdaptiveCleanUpProxy;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;
import com.github.ymwangzq.config.adapter.facade.DelayCleanUpConfig;
import com.github.ymwangzq.config.adapter.facade.EventSource;
import com.google.common.base.Preconditions;

/**
 * <p>
 * {@link ConfigAdapters#createMapOnEachUpdate} 创建的 ConfigAdapter 任何变更事件都会通知下游，不管当前值是否真的发生变化
 * {@link ConfigAdapters#create} 创建的 ConfigAdapter 只在上游的对象发生变更的时候，才会调用下游的 mapper; 使用 {@link java.util.Objects#equals}
 * 判断上游对象是否变更
 *
 * @author myco
 * Created on 2019-11-10
 */
public class ConfigAdapters {

    private ConfigAdapters() {
    }

    @Nonnull
    public static <T> ConfigAdapter<T> toOnlyMapOnUpstreamChangeConfigAdapter(@Nonnull ConfigAdapter<T> configAdapter) {
        Preconditions.checkNotNull(configAdapter);
        if (configAdapter instanceof OnlyMapOnUpstreamChangeConfigAdapter) {
            return configAdapter;
        } else {
            return new OnlyMapOnUpstreamChangeConfigAdapter<>(configAdapter);
        }
    }

    @Nonnull
    public static <T> ConfigAdapter<T> toMapOnEachUpdateConfigAdapter(@Nonnull ConfigAdapter<T> configAdapter) {
        Preconditions.checkNotNull(configAdapter);
        if (configAdapter instanceof OnlyMapOnUpstreamChangeConfigAdapter) {
            return ((OnlyMapOnUpstreamChangeConfigAdapter<T>) configAdapter).origin();
        } else {
            return configAdapter;
        }
    }

    public static <T> ConfigAdapter<T> createMapOnEachUpdate(@Nonnull EventSource eventSource,
                                                             @Nonnull Supplier<T> fetcher, @Nonnull Consumer<T> cleanUp) {
        return BaseConfigAdapter.doCreate(eventSource, fetcher, Function.identity(), cleanUp);
    }

    public static <T> ConfigAdapter<T> createMapOnEachUpdate(@Nonnull EventSource eventSource,
                                                             @Nonnull Supplier<T> fetcher, @Nonnull Consumer<T> cleanUp,
                                                             @Nullable DelayCleanUpConfig delayCleanUpConfig) {
        return BaseConfigAdapter.doCreate(eventSource, fetcher, cleanUp, delayCleanUpConfig);
    }

    /**
     * @see AdaptiveCleanUpProxy
     */
    public static <O, T extends AdaptiveCleanUpProxy<O>> ConfigAdapter<T> createAdaptiveCleanUp(
            @Nonnull EventSource eventSource, @Nonnull Supplier<T> fetcher, @Nonnull Consumer<O> cleanUp) {
        return ConfigAdapters.toOnlyMapOnUpstreamChangeConfigAdapter(BaseConfigAdapter.doCreate(eventSource, fetcher,
                t -> Optional.ofNullable(t).map(AdaptiveCleanUpProxy::getDelegate).orElse(null), cleanUp));
    }

    public static <T> ConfigAdapter<T> create(@Nonnull EventSource eventSource,
                                              @Nonnull Supplier<T> fetcher, @Nonnull Consumer<T> cleanUp) {
        return ConfigAdapters
                .toOnlyMapOnUpstreamChangeConfigAdapter(createMapOnEachUpdate(eventSource, fetcher, cleanUp));
    }

    public static <T> ConfigAdapter<T> create(@Nonnull EventSource eventSource,
                                              @Nonnull Supplier<T> fetcher, @Nonnull Consumer<T> cleanUp,
                                              @Nullable DelayCleanUpConfig delayCleanUpConfig) {
        return ConfigAdapters
                .toOnlyMapOnUpstreamChangeConfigAdapter(
                        createMapOnEachUpdate(eventSource, fetcher, cleanUp, delayCleanUpConfig));
    }

    /**
     * 合并两个 ConfigAdapter，要合并多个的话，自己照着下边代码写吧
     */
    public static <F1, F2, T> ConfigAdapter<T> mergeTwoConfigAdapters(@Nonnull ConfigAdapter<F1> configAdapter1,
                                                                      @Nonnull ConfigAdapter<F2> configAdapter2, @Nonnull BiFunction<F1, F2, T> mergeFunction,
                                                                      @Nonnull Consumer<T> cleanUp) {
        return mergeTwoConfigAdapters(configAdapter1, configAdapter2, mergeFunction, cleanUp, null);
    }

    public static <F1, F2, T> ConfigAdapter<T> mergeTwoConfigAdapters(@Nonnull ConfigAdapter<F1> configAdapter1,
                                                                      @Nonnull ConfigAdapter<F2> configAdapter2, @Nonnull BiFunction<F1, F2, T> mergeFunction,
                                                                      @Nonnull Consumer<T> cleanUp, @Nullable DelayCleanUpConfig delayCleanUpConfig) {
        Preconditions.checkNotNull(configAdapter1);
        Preconditions.checkNotNull(configAdapter2);
        Preconditions.checkNotNull(mergeFunction);
        Preconditions.checkNotNull(cleanUp);

        return ConfigAdapters.create(EventSourceUtil.anyOf(configAdapter1, configAdapter2),
                () -> mergeFunction.apply(configAdapter1.get(), configAdapter2.get()), cleanUp, delayCleanUpConfig);
    }

    /**
     * flatMap 方法功能如下：
     * 1. 首先读配置1，根据配置1的内容决定是否读配置2
     * 2. 配置1和配置2的变更都能够收到通知
     * <p>
     * 注意：返回的 ConfigAdapter 的更新并不是实时的，而是每秒轮询一次触发更新。能够保证最终一致
     */
    @Nonnull
    public static <F, T> ConfigAdapter<T> flatMap(@Nonnull ConfigAdapter<F> configAdapter,
                                                  Function<F, ConfigAdapter<T>> mapFunction) {

        ConfigAdapter<ConfigAdapter<T>> resultConfigAdapter =
                configAdapter.map(MapWithLastValueMapper.wrap((lastFrom, lastSubConfigAdapter, newFrom) -> {
                    ConfigAdapter<T> newSubConfigAdapter = mapFunction.apply(newFrom);
                    if (newSubConfigAdapter == null) {
                        newSubConfigAdapter = SupplierConfigAdapter.createConfigAdapter(() -> null);
                    }
                    if (newSubConfigAdapter == lastSubConfigAdapter) {
                        return newSubConfigAdapter;
                    }
                    if (newSubConfigAdapter == configAdapter) {
                        return newSubConfigAdapter;
                    }
                    return newSubConfigAdapter;
                }));

        return SupplierConfigAdapter.createConfigAdapter(() -> Objects.requireNonNull(resultConfigAdapter.get()).get());
    }

    /**
     * 将传入的所有 ConfigAdapter 合并成一个 ConfigAdapter，将入参所有 ConfigAdapter 的值放入一个 list。
     * 入参中任何一个 ConfigAdapter 变更，返回的 list 都会收到更新
     */
    @Nonnull
    public static <T> ConfigAdapter<List<T>> allAsList(@Nonnull Iterable<ConfigAdapter<T>> iterable) {
        EventSource eventSource = EventSourceUtil.anyOf(iterable);
        return ConfigAdapters.create(eventSource, () -> StreamSupport.stream(iterable.spliterator(), false)
                .map(configAdapter -> Optional.ofNullable(configAdapter)
                        .map(ConfigAdapter::get)
                        .orElse(null))
                .collect(Collectors.toList()), r -> {
        });
    }
}
