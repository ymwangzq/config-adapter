package com.github.ymwangzq.config.adapter.core;

import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ymwangzq.config.adapter.facade.ConfigAdapterMapper;
import com.github.ymwangzq.config.adapter.facade.DelayCleanUpConfig;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * 使用这个 mapper 的 ConfigAdapter 的行为如下：
 * 刚构造好 ConfigAdapter 后，调用 get 方法如果有异常会直接抛出，直到 get 成功拿到结果后，后续 get 会直接返回之前成功的结果；（这是 ConfigAdapter 的行为）
 * 后续 ConfigAdapter 在收到更新事件的时候，如果 map 的时候有异常，默认行为是不更新，也没有重试
 * 使用这个 mapper 后，在收到更新事件的时候，如果 map 抛出异常，则会进行重试；直到重试成功, 或者等到下一次更新事件到来；
 * <p>
 * 重试过程中，下一次更新事件到来时，重试线程会收到 InterruptedException; map 方法中应当谨慎处理好这种情况
 * <p>
 * 备注：一些 interrupt 可能导致问题的地方
 * 1) org.apache.curator.utils.ThreadUtils#checkInterrupted(java.lang.Throwable) curator 里会保持 interrupt state
 *
 * @author myco
 * Created on 2019-12-10
 */
@NotThreadSafe
public class UpdateWithRetryMapper<From, To> implements ConfigAdapterMapper<From, To> {

    private static final Logger logger = LoggerFactory.getLogger(UpdateWithRetryMapper.class);

    public interface RetryConfig {
        long NO_RETRY = -1;

        /**
         * @return NO_RETRY to abort
         */
        long nextRetryBackOffMs(int retryTime, Throwable lastTryException);
    }

    public static class RetryForever implements RetryConfig {

        private final double backOffExp;
        private final long initBackOffMs;
        private final long maxBackOffMs;
        private final Predicate<Throwable> abortRetry;

        public RetryForever(double backOffExp, long initBackOffMs, long maxBackOffMs,
                @Nullable Predicate<Throwable> abortRetry) {
            Preconditions.checkArgument(backOffExp > 1, "backOffExp 需要大于 1");
            this.backOffExp = backOffExp;
            this.initBackOffMs = initBackOffMs;
            this.maxBackOffMs = maxBackOffMs;
            this.abortRetry = abortRetry;
        }

        @Override
        public long nextRetryBackOffMs(int retryTime, Throwable lastTryException) {
            if (abortRetry != null && abortRetry.test(lastTryException)) {
                return NO_RETRY;
            }
            return (long) Math.min(maxBackOffMs, initBackOffMs * Math.pow(backOffExp, retryTime));
        }
    }

    public static class RetryConfigImpl extends RetryForever {

        private final int maxRetryTime;

        public RetryConfigImpl(int maxRetryTime, double backOffExp, long initBackOffMs, long maxBackOffMs,
                @Nullable Predicate<Throwable> abortRetry) {
            super(backOffExp, initBackOffMs, maxBackOffMs, abortRetry);
            this.maxRetryTime = maxRetryTime;
        }

        @Override
        public long nextRetryBackOffMs(int retryTime, Throwable lastTryException) {
            if (retryTime > maxRetryTime) {
                return NO_RETRY;
            }
            return super.nextRetryBackOffMs(retryTime, lastTryException);
        }
    }

    static class RetryHelper {

        static <T> T withRetry(Supplier<T> supplier, RetryConfig retryConfig) {
            int retryTime = 0;
            long backOffMs = 0;
            while (backOffMs >= 0) {
                // 先检测下 interrupt 标记，被 interrupt 就不重试了，同时清理掉 interrupt 标记，interrupt 异常在 root cause 里
                try {
                    TimeUnit.MILLISECONDS.sleep(backOffMs);
                } catch (InterruptedException t) {
                    throw new RuntimeException("interrupted", t);
                }
                Throwable currentTryException = null;
                try {
                    // 成功就直接 return
                    return supplier.get();
                } catch (Throwable t) {
                    if (Throwables.getRootCause(t) instanceof InterruptedException) {
                        // InterruptedException 直接抛出去
                        throw t;
                    }
                    currentTryException = t;
                    logger.warn("配置更新失败，当前是第 {} 次重试, [{}]", retryTime, supplier, t);
                }
                retryTime++;
                backOffMs = retryConfig.nextRetryBackOffMs(retryTime, currentTryException);
            }
            throw new RuntimeException("到达重试次数上限 " + retryTime + " 仍然失败，" + supplier);
        }
    }

    private final ConfigAdapterMapper<From, To> delegate;
    private final RetryConfig retryConfig;

    private volatile boolean activated = false;

    public static <From, To> ConfigAdapterMapper<From, To> wrap(ConfigAdapterMapper<From, To> delegate,
                                                                RetryConfig retryConfig) {
        return new UpdateWithRetryMapper<>(delegate, retryConfig);
    }

    private UpdateWithRetryMapper(ConfigAdapterMapper<From, To> delegate, RetryConfig retryConfig) {
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
            return RetryHelper.withRetry(new Supplier<To>() {
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
    public void cleanUp(@Nullable To to) {
        delegate.cleanUp(to);
    }

    @Nullable
    @Override
    public DelayCleanUpConfig delayCleanUpConfig() {
        return delegate.delayCleanUpConfig();
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
