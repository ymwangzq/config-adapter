package com.github.ymwangzq.config.adapter.core;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.github.ymwangzq.config.adapter.facade.ConfigAdapterMapper;
import com.github.ymwangzq.config.adapter.facade.DelayCleanUpConfig;
import com.google.common.base.MoreObjects;

/**
 * 不能复用 MapWithLastValueMapper 对象, 创建出来只能用在一个地方
 * 多次使用需要多次创建
 *
 * @author myco
 * Created on 2019-11-10
 */
@NotThreadSafe
public class MapWithLastValueMapper<From, To> implements ConfigAdapterMapper<From, To> {

    public interface MapWithLastValue<From, To> {

        @Nullable
        To mapWithLastValue(@Nullable From lastFrom, @Nullable To lastTo, @Nullable From newFrom);

        default void cleanUp(@Nullable To to) {
        }

        @Nullable
        default DelayCleanUpConfig delayCleanUpConfig() {
            return null;
        }
    }

    private final MapWithLastValue<From, To> delegate;

    private volatile From lastFromValue = null;
    private volatile To lastToValue = null;

    public static <From, To> MapWithLastValueMapper<From, To> wrap(MapWithLastValue<From, To> delegate) {
        return new MapWithLastValueMapper<>(delegate);
    }

    private MapWithLastValueMapper(
            MapWithLastValue<From, To> delegate) {
        this.delegate = delegate;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("delegate", delegate)
                .add("lastFromValue", lastFromValue)
                .add("lastToValue", lastToValue)
                .toString();
    }

    @Override
    @Nullable
    public To map(@Nullable From from) {
        //noinspection NonAtomicOperationOnVolatileField
        lastToValue = delegate.mapWithLastValue(lastFromValue, lastToValue, from);
        lastFromValue = from;
        return lastToValue;
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
