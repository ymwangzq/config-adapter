package com.github.ymwangzq.config.adapter.core;

import javax.annotation.Nullable;

import com.github.ymwangzq.config.adapter.facade.ConfigAdapterMapper;
import com.github.ymwangzq.config.adapter.facade.DelayCleanUpConfig;

/**
 * @author myco
 * Created on 2020-11-18
 */
public class DelegatingConfigAdapterMapper<From, To> implements ConfigAdapterMapper<From, To> {

    private final ConfigAdapterMapper<From, To> delegate;

    public DelegatingConfigAdapterMapper(ConfigAdapterMapper<From, To> delegate) {
        this.delegate = delegate;
    }

    protected ConfigAdapterMapper<From, To> getDelegate() {
        return delegate;
    }

    @Override
    @Nullable
    public To map(@Nullable From from) {
        return getDelegate().map(from);
    }

    @Override
    @Nullable
    public To transformToRealCleanUp(@Nullable To to) {
        return getDelegate().transformToRealCleanUp(to);
    }

    @Override
    public void cleanUp(@Nullable To to) {
        getDelegate().cleanUp(to);
    }

    @Override
    @Nullable
    public DelayCleanUpConfig delayCleanUpConfig() {
        return getDelegate().delayCleanUpConfig();
    }
}
