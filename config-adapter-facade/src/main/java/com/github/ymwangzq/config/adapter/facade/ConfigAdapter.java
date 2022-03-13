package com.github.ymwangzq.config.adapter.facade;

import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * 动态配置统一接口
 *
 * @author myco
 * Created on 2019-10-10
 */
public interface ConfigAdapter<T> extends EventSource, Supplier<T>, AutoCloseable {

    /**
     * 获取配置内容
     */
    @Nullable
    T get();

    /**
     * 将动态配置映射成新的动态配置/动态资源
     */
    @Nonnull
    <R> ConfigAdapter<R> map(@Nonnull ConfigAdapterMapper<? super T, R> configMapper);

    /**
     * 将动态配置映射成新的动态配置/动态资源
     * 这个方法支持自适应 cleanUp，相关介绍见 {@link AdaptiveCleanUpProxy}
     */
    @Nonnull
    default <R, C> ConfigAdapter<R> map(@Nonnull AdaptiveCleanUpConfigAdapterMapper<? super T, R, C> configMapper) {
        return map(new ConfigAdapterMapper<T, R>() {
            @Nullable
            @Override
            public R map(@Nullable T t) {
                return configMapper.map(t);
            }

            @Override
            public void cleanUp(@Nullable R r) {
                configMapper.cleanUp(configMapper.transformToRealCleanUp(r));
            }
        });
    }

    /**
     * 立即断开对上游 EventSource 的监听，并清理当前内部持有的资源
     * 此后，当前 ConfigAdapter 不应当再继续被引用，应当被 gc 掉。此后的所有操作都会抛出异常
     * <p>
     * 通常情况下，不需要手动 close。在壳对象被 gc 掉以后，会自动清理内部持有的资源
     */
    @Override
    void close() throws Exception;
}
