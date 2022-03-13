package com.github.ymwangzq.config.adapter.core;

import java.util.function.Supplier;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.github.ymwangzq.config.adapter.facade.AdaptiveCleanUpConfigAdapterMapper;

/**
 * @see UpdateWithRetryMapper
 *
 * @author myco
 * Created on 2019-12-10
 */
@NotThreadSafe
public class AdaptiveCleanUpUpdateWithRetryMapper<From, To, CleanUp>
        implements AdaptiveCleanUpConfigAdapterMapper<From, To, CleanUp> {

    private static final Logger logger = LoggerFactory.getLogger(AdaptiveCleanUpUpdateWithRetryMapper.class);

    private final AdaptiveCleanUpConfigAdapterMapper<From, To, CleanUp> delegate;
    private final UpdateWithRetryMapper.RetryConfig retryConfig;

    private volatile boolean activated = false;

    public static <From, To, CleanUp> AdaptiveCleanUpConfigAdapterMapper<From, To, CleanUp> wrap(
            AdaptiveCleanUpConfigAdapterMapper<From, To, CleanUp> delegate,
            UpdateWithRetryMapper.RetryConfig retryConfig) {
        return new AdaptiveCleanUpUpdateWithRetryMapper<>(delegate, retryConfig);
    }

    private AdaptiveCleanUpUpdateWithRetryMapper(AdaptiveCleanUpConfigAdapterMapper<From, To, CleanUp> delegate,
                                                 UpdateWithRetryMapper.RetryConfig retryConfig) {
        this.delegate = delegate;
        this.retryConfig = retryConfig;
    }

    @Nullable
    @Override
    public To map(@Nullable From from) {
        if (!activated) {
            // 没有成功初始化过，有异常就直接抛出去
            To to = delegate.map(from);
            activated = true;
            return to;
        } else {
            // 初始化成功了，再次调到这里是配置更新，这会儿开始按照配置的重试策略开始重试
            return UpdateWithRetryMapper.RetryHelper.withRetry(new Supplier<To>() {
                @Override
                public To get() {
                    return delegate.map(from);
                }

                @Override
                public String toString() {
                    return delegate.toString();
                }
            }, retryConfig);
        }
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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("delegate", delegate)
                .add("retryConfig", retryConfig)
                .add("activated", activated)
                .toString();
    }
}
