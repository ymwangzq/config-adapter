package com.github.ymwangzq.config.adapter.facade;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.LongSupplier;

import javax.annotation.Nonnull;

/**
 * @author myco
 * Created on 2019-10-27
 */
public interface DelayCleanUpConfig {

    @Nonnull
    ScheduledExecutorService delayCleanUpExecutor();

    @Nonnull
    LongSupplier cleanUpDelayMs();
}
