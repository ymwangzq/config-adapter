package com.github.ymwangzq.config.adapter.core;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.github.ymwangzq.config.adapter.facade.AdaptiveCleanUpConfigAdapterMapper;
import com.google.common.base.MoreObjects;

/**
 * @see MapWithLastValueMapper
 *
 * @author myco
 * Created on 2019-11-10
 */
@NotThreadSafe
public class AdaptiveCleanUpMapWithLastValueMapper<From, To, CleanUp>
        implements AdaptiveCleanUpConfigAdapterMapper<From, To, CleanUp> {

    public interface AdaptiveCleanUpMapWithLastValue<From, To, CleanUp> {

        @Nullable
        To mapWithLastValue(@Nullable From lastFrom, @Nullable To lastTo, @Nullable From newFrom);

        default void cleanUp(@Nullable CleanUp cleanUp) {
        }

        @Nullable
        default CleanUp transformToRealCleanUp(@Nullable To to) {
            return null;
        }
    }

    private final AdaptiveCleanUpMapWithLastValue<From, To, CleanUp> delegate;

    private volatile From lastFromValue = null;
    private volatile To lastToValue = null;

    public static <From, To, CleanUp> AdaptiveCleanUpMapWithLastValueMapper<From, To, CleanUp> wrap(
            AdaptiveCleanUpMapWithLastValue<From, To, CleanUp> delegate) {
        return new AdaptiveCleanUpMapWithLastValueMapper<>(delegate);
    }

    private AdaptiveCleanUpMapWithLastValueMapper(
            AdaptiveCleanUpMapWithLastValue<From, To, CleanUp> delegate) {
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
    public void cleanUp(@Nullable CleanUp cleanUp) {
        delegate.cleanUp(cleanUp);
    }

    @Override
    @Nullable
    public CleanUp transformToRealCleanUp(@Nullable To to) {
        return delegate.transformToRealCleanUp(to);
    }
}
