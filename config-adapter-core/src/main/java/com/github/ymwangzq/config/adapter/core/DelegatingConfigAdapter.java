package com.github.ymwangzq.config.adapter.core;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.github.ymwangzq.config.adapter.facade.AdaptiveCleanUpConfigAdapterMapper;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapterMapper;
import com.github.ymwangzq.config.adapter.facade.EventListener;

/**
 * @author myco
 * Created on 2019-11-11
 */
public class DelegatingConfigAdapter<T> implements ConfigAdapter<T> {

    private final ConfigAdapter<T> delegate;

    // 持有一些引用，避免被 gc
    private final Object[] refs;

    public DelegatingConfigAdapter(ConfigAdapter<T> delegate) {
        this(delegate, (Object[]) null);
    }

    private DelegatingConfigAdapter(ConfigAdapter<T> delegate, Object... refs) {
        Preconditions.checkNotNull(delegate);
        this.delegate = delegate;
        this.refs = refs;
    }

    /**
     * 不要通过逐级调用 getDelegate 来拿到最原始的 BaseConfigAdapter；一旦 BaseConfigAdapter 的引用泄漏出去
     * 可能导致不可预期的问题
     */
    @Nonnull
    final ConfigAdapter<T> getDelegate() {
        return delegate;
    }

    @Override
    @Nullable
    public T get() {
        return delegate.get();
    }

    @Override
    @Nonnull
    public <R> ConfigAdapter<R> map(@Nonnull
                                            ConfigAdapterMapper<? super T, R> configMapper) {
        // 拿一下 map 之前的 ConfigAdapter 壳引用，避免被 gc 干掉
        return new DelegatingConfigAdapter<>(delegate.map(configMapper), this);
    }

    @Nonnull
    @Override
    public <R, C> ConfigAdapter<R> map(@Nonnull AdaptiveCleanUpConfigAdapterMapper<? super T, R, C> configMapper) {
        // 拿一下 map 之前的 ConfigAdapter 壳引用，避免被 gc 干掉
        return new DelegatingConfigAdapter<>(delegate.map(configMapper), this);
    }

    @Override
    public void addEventListener(
            @Nonnull EventListener eventListener) {
        delegate.addEventListener(eventListener);
    }

    @Override
    public void removeEventListener(
            @Nonnull EventListener eventListener) {
        delegate.removeEventListener(eventListener);
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
