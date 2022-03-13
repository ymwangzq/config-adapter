package com.github.ymwangzq.config.adapter.facade;

import javax.annotation.Nullable;

/**
 * 支持自适应 cleanUp 的动态配置转换器
 *
 * 自适应 cleanUp 介绍见 {@link AdaptiveCleanUpProxy}
 *
 * @author myco
 * Created on 2020-08-20
 */
public interface AdaptiveCleanUpConfigAdapterMapper<From, To, CleanUp> {

    /**
     * 动态配置变换逻辑
     */
    @Nullable
    To map(@Nullable From from);

    /**
     * 真正的清理逻辑
     */
    default void cleanUp(@Nullable CleanUp cleanUp) {
    }

    /**
     * 获取 To 对象内部真正需要清理的 CleanUp 对象
     */
    @Nullable
    default CleanUp transformToRealCleanUp(@Nullable To to) {
        return null;
    }
}
