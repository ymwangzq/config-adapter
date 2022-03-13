package com.github.ymwangzq.config.adapter.core;

import java.util.concurrent.Callable;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author myco
 * Created on 2021-05-29
 */
public interface SingleThreadConfigDispatchExecutor {

    /**
     * 提交配置变更任务，并中断当前在执行的线程。如果任务堆积，只保留最新的任务。因为配置变更场景不关心中间的所有状态，只关心最新状态。
     */
    <T> ListenableFuture<T> interruptCurrentAndSubmit(Callable<T> callable);

    void shutdown();
}
