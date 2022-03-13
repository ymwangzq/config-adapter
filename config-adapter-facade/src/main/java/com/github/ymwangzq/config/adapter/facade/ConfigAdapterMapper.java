package com.github.ymwangzq.config.adapter.facade;

import javax.annotation.Nullable;

/**
 * 简单的动态配置转换器，不支持自适应 cleanUp
 *
 * @author myco
 * Created on 2019-10-11
 */
public interface ConfigAdapterMapper<From, To> extends AdaptiveCleanUpConfigAdapterMapper<From, To, To> {

    /**
     * {@inheritDoc}
     */
    @Nullable
    To map(@Nullable From from);

    /**
     * {@inheritDoc}
     */
    @Nullable
    @Override
    default To transformToRealCleanUp(@Nullable To to) {
        return to;
    }

    /**
     * {@inheritDoc}
     */
    default void cleanUp(@Nullable To to) {
    }

    /**
     * 延迟 cleanUp 配置
     */
    @Nullable
    default DelayCleanUpConfig delayCleanUpConfig() {
        return null;
    }
}
