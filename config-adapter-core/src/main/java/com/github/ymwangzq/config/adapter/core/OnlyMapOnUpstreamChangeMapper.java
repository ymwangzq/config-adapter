package com.github.ymwangzq.config.adapter.core;

import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.github.ymwangzq.config.adapter.facade.ConfigAdapterMapper;
import com.github.ymwangzq.config.adapter.facade.DelayCleanUpConfig;
import com.google.common.base.MoreObjects;

/**
 * 使用这个 mapper 后，ConfigAdapter 不会在每次 eventSource 收到 updateEvent 的时候都触发 map 操作
 * 只在发现上游的值变更的时候，才会执行变更操作
 *
 * 不能复用 OnlyMapOnUpstreamChangeMapper 对象, 创建出来只能用在一个地方
 * 多次使用需要多次创建
 *
 * @author myco
 * Created on 2019-11-09
 */
@NotThreadSafe
class OnlyMapOnUpstreamChangeMapper<From, To> implements ConfigAdapterMapper<From, To> {

    private final ConfigAdapterMapper<From, To> delegate;

    private volatile boolean activated = false;
    private volatile From lastFromValue = null;
    private volatile To lastToValue = null;

    public static <From, To> ConfigAdapterMapper<From, To> wrap(ConfigAdapterMapper<From, To> delegate) {
        return new OnlyMapOnUpstreamChangeMapper<>(delegate);
    }

    private OnlyMapOnUpstreamChangeMapper(ConfigAdapterMapper<From, To> delegate) {
        this.delegate = delegate;
    }

    @Nullable
    @Override
    public final To map(@Nullable From from) {
        From oldFrom = lastFromValue;
        if (!activated) {
            activated = true;
            // map 成功后，才更新 lastToValue & lastFromValue
            lastToValue = delegate.map(from);
            lastFromValue = from;
            return lastToValue;
        } else {
            if (Objects.equals(oldFrom, from)) {
                return lastToValue;
            } else {
                // map 成功后，才更新 lastToValue & lastFromValue
                lastToValue = delegate.map(from);
                lastFromValue = from;
                return lastToValue;
            }
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("delegate", delegate)
                .add("activated", activated)
                .add("lastFromValue", lastFromValue)
                .add("lastToValue", lastToValue)
                .toString();
    }

    @Override
    public void cleanUp(@Nullable To to) {
        delegate.cleanUp(to);
    }

    @Override
    @Nullable
    public DelayCleanUpConfig delayCleanUpConfig() {
        return delegate.delayCleanUpConfig();
    }
}
