package com.github.ymwangzq.config.adapter.core;

import javax.annotation.Nonnull;

import com.github.ymwangzq.config.adapter.facade.ConfigAdapterMapper;
import com.github.ymwangzq.config.adapter.facade.AdaptiveCleanUpConfigAdapterMapper;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;

/**
 * @see OnlyMapOnUpstreamChangeMapper
 *
 * @author myco
 * Created on 2019-11-10
 */
public class OnlyMapOnUpstreamChangeConfigAdapter<T> extends DelegatingConfigAdapter<T> {

    public OnlyMapOnUpstreamChangeConfigAdapter(@Nonnull ConfigAdapter<T> delegate) {
        super(delegate);
    }

    @Override
    @Nonnull
    public <R> ConfigAdapter<R> map(
            @Nonnull ConfigAdapterMapper<? super T, R> configMapper) {
        return new OnlyMapOnUpstreamChangeConfigAdapter<>(
                getDelegate().map(OnlyMapOnUpstreamChangeMapper.wrap(configMapper)));
    }

    @Nonnull
    @Override
    public <R, C> ConfigAdapter<R> map(@Nonnull AdaptiveCleanUpConfigAdapterMapper<? super T, R, C> configMapper) {
        return new OnlyMapOnUpstreamChangeConfigAdapter<>(
                getDelegate().map(AdaptiveCleanUpOnlyMapOnUpstreamChangeMapper.wrap(configMapper)));
    }

    ConfigAdapter<T> origin() {
        return getDelegate();
    }
}
