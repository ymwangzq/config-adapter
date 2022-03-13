package com.github.ymwangzq.config.adapter.core;

import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.github.ymwangzq.config.adapter.facade.AdaptiveCleanUpConfigAdapterMapper;
import com.google.common.base.MoreObjects;

/**
 * @see OnlyMapOnUpstreamChangeMapper
 *
 * @author myco
 * Created on 2019-11-09
 */
@NotThreadSafe
class AdaptiveCleanUpOnlyMapOnUpstreamChangeMapper<From, To, CleanUp>
        implements AdaptiveCleanUpConfigAdapterMapper<From, To, CleanUp> {

    private final AdaptiveCleanUpConfigAdapterMapper<From, To, CleanUp> delegate;

    private volatile boolean activated = false;
    private volatile From lastFromValue = null;
    private volatile To lastToValue = null;

    public static <From, To, CleanUp> AdaptiveCleanUpConfigAdapterMapper<From, To, CleanUp> wrap(
            AdaptiveCleanUpConfigAdapterMapper<From, To, CleanUp> delegate) {
        return new AdaptiveCleanUpOnlyMapOnUpstreamChangeMapper<>(delegate);
    }

    private AdaptiveCleanUpOnlyMapOnUpstreamChangeMapper(
            AdaptiveCleanUpConfigAdapterMapper<From, To, CleanUp> delegate) {
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
    public void cleanUp(@Nullable CleanUp cleanUp) {
        delegate.cleanUp(cleanUp);
    }

    @Override
    @Nullable
    public CleanUp transformToRealCleanUp(@Nullable To to) {
        return delegate.transformToRealCleanUp(to);
    }
}
