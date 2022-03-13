package com.github.ymwangzq.config.adapter.core;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author myco
 * Created on 2021-05-29
 */
public class SingleThreadConfigDispatchExecutorImpl implements SingleThreadConfigDispatchExecutor {

    private static final Logger logger = LoggerFactory.getLogger(SingleThreadConfigDispatchExecutorImpl.class);

    private static class FutureTask<T> {
        private final SettableFuture<T> resultFuture = SettableFuture.create();
        private final Callable<T> callable;

        private FutureTask(Callable<T> callable) {
            this.callable = callable;
        }

        private void run() {
            if (resultFuture.isCancelled()) {
                return;
            }
            try {
                resultFuture.set(callable.call());
            } catch (Throwable t) {
                resultFuture.setException(t);
            }
        }
    }

    private enum WorkerStatus {
        init,
        running,
        exits
    }

    private static class Worker implements Runnable {

        private static <T extends Throwable> T unknownStackTrace(T cause, Class<?> clazz, String method) {
            cause.setStackTrace(new StackTraceElement[] {new StackTraceElement(clazz.getName(), method, null, -1)});
            return cause;
        }

        private static final CancellationException CANCELLATION_EXCEPTION =
                unknownStackTrace(new CancellationException(), SingleThreadConfigDispatchExecutorImpl.class,
                        "submit(...)");

        private final AtomicReference<WorkerStatus> status = new AtomicReference<>(WorkerStatus.init);

        private final BlockingQueue<FutureTask<?>> taskQueue = new ArrayBlockingQueue<>(1, true);

        private final Thread thread;
        private final int keepAliveTime;
        private final TimeUnit keepAliveTimeUnit;
        private final AtomicReference<Worker> workerHolder;

        private Worker(ThreadFactory threadFactory, int keepAliveTime, TimeUnit keepAliveTimeUnit,
                AtomicReference<Worker> workerHolder) {
            this.keepAliveTime = keepAliveTime;
            this.keepAliveTimeUnit = keepAliveTimeUnit;
            this.workerHolder = workerHolder;
            this.thread = threadFactory.newThread(this);
        }

        private void ensureStart() {
            if (status.compareAndSet(WorkerStatus.init, WorkerStatus.running)) {
                this.thread.start();
            }
        }

        private boolean interruptAndAddTask(FutureTask<?> futureTask) {
            switch (status.get()) {
                case exits:
                    // 线程已经消亡了，返回 false。需要外层重新创建 worker 并重新 addTask
                    return false;
                case init:
                case running:
                    // 当前线程还在跑，加锁然后重新检查状态
                    synchronized (status) {
                        switch (status.get()) {
                            case exits:
                                return false;
                            case init:
                            case running:
                                // 队列里还有任务，直接拿出来，不要再执行了。
                                FutureTask<?> discardTask = taskQueue.poll();
                                if (discardTask != null) {
                                    // 给丢弃的任务设置异常
                                    discardTask.resultFuture.setException(CANCELLATION_EXCEPTION);
                                }
                                // 队列长度只有 1，所以这里应当空了
                                Preconditions.checkState(taskQueue.isEmpty(), "BUG!");
                                // 尝试中断当前 worker 线程
                                thread.interrupt();
                                // 丢弃最老的任务，然后将新的任务入队
                                boolean addSuccess = taskQueue.offer(futureTask);
                                Preconditions.checkState(addSuccess, "BUG!");
                                return true;
                            default:
                                throw new IllegalStateException("BUG!");
                        }
                    }
                default:
                    // 这些情况都不应该出现，抛个异常
                    throw new IllegalStateException("BUG!");
            }
        }

        private FutureTask<?> getTaskAndUpdateWorkerStatus() {
            while (true) {
                try {
                    FutureTask<?> futureTask = taskQueue.poll(keepAliveTime, keepAliveTimeUnit);
                    if (futureTask == null) {
                        // 当前队列里没有任务了，而且也等待了 keepAlive 这么多时间，那么需要允许当前线程消亡。同时又要保证后续提交的任务一定能够拉起线程。
                        synchronized (status) {
                            futureTask = taskQueue.poll();
                            if (futureTask == null) {
                                status.set(WorkerStatus.exits);
                            }
                        }
                    }
                    return futureTask;
                } catch (InterruptedException t) {
                    // 被 interrupt 需要重试
                }
            }
        }

        @Override
        public void run() {
            while (status.get() == WorkerStatus.running) {
                FutureTask<?> futureTask = getTaskAndUpdateWorkerStatus();
                if (futureTask != null) {
                    // 清理 interrupt 标记，避免污染
                    //noinspection ResultOfMethodCallIgnored
                    Thread.interrupted();
                    futureTask.run();
                }
            }
            if (!taskQueue.isEmpty()) {
                logger.error("", new IllegalStateException("BUG!"));
            }
            workerHolder.compareAndSet(this, null);
        }

        private WorkerStatus getStatus() {
            return status.get();
        }

        private int pendingTasks() {
            return taskQueue.size();
        }
    }

    private final AtomicReference<Worker> workerHolder = new AtomicReference<>(null);

    private final ThreadFactory threadFactory;
    private final int keepAliveTime;
    private final TimeUnit keepAliveTimeUnit;

    public SingleThreadConfigDispatchExecutorImpl(ThreadFactory threadFactory, int keepAliveTime,
            TimeUnit keepAliveTimeUnit) {
        this.threadFactory = threadFactory;
        this.keepAliveTime = keepAliveTime;
        this.keepAliveTimeUnit = keepAliveTimeUnit;
    }

    private Worker newWorker() {
        synchronized (workerHolder) {
            Worker worker = new Worker(threadFactory, keepAliveTime, keepAliveTimeUnit, workerHolder);
            workerHolder.set(worker);
            return worker;
        }
    }

    private void checkWorkerAndInterruptThenAddTask(FutureTask<?> futureTask) {
        Worker worker = null;
        boolean addTaskSuccess = false;
        while (!addTaskSuccess) {
            worker = this.workerHolder.get();
            if (worker == null || worker.getStatus() == WorkerStatus.exits) {
                // worker 不存在，就创建一个新的 worker
                worker = newWorker();
            }
            addTaskSuccess = worker.interruptAndAddTask(futureTask);
            if (worker.getStatus() == WorkerStatus.exits && worker.pendingTasks() > 0) {
                throw new IllegalStateException("BUG!");
            }
        }
        // 确保 worker 开始执行
        worker.ensureStart();
    }

    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    @Override
    public <T> ListenableFuture<T> interruptCurrentAndSubmit(Callable<T> callable) {
        if (shutdown.get()) {
            throw new RejectedExecutionException("Executor already shutdown!");
        }
        FutureTask<T> futureTask = new FutureTask<>(callable);
        checkWorkerAndInterruptThenAddTask(futureTask);
        return futureTask.resultFuture;
    }

    @Override
    public void shutdown() {
        shutdown.set(true);
    }
}
