package org.apache.ignite.internal;

import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.mxbean.StrippedExecutorMXBean;

import java.util.concurrent.ExecutorService;

/**
 * Adapter for {@link org.apache.ignite.mxbean.StrippedExecutorMXBean} which delegates all method calls to the underlying
 * {@link ExecutorService} instance.
 */
public class StrippedExecutorMXBeanAdapter implements StrippedExecutorMXBean {

    private final ExecutorService exec;

    /**
     * Creates adapter.
     *
     * @param exec Executor service
     */
    public StrippedExecutorMXBeanAdapter(ExecutorService exec) {
        assert exec != null;

        this.exec = exec;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void checkStarvation() {
        ((StripedExecutor) exec).checkStarvation();
    }

    @Override
    public int getStripesCount() {
        return ((StripedExecutor) exec).stripes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isShutdown() {
        return exec.isShutdown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTerminated() {
        return exec.isTerminated();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getTotalQueueSize() {
        return ((StripedExecutor) exec).queueSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTotalCompletedTasksCount() {
        return ((StripedExecutor) exec).completedTasks();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long[] getStripesCompletedTasksCount() {
        return ((StripedExecutor) exec).stripesCompletedTasks();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getActiveCount() {
        return ((StripedExecutor) exec).activeStripesCount();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean[] getStripesActiveStatuses() {
        return ((StripedExecutor) exec).stripesActiveStatuses();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int[] getStripesQueueSizes() {
        return ((StripedExecutor) exec).stripesQueueSizes();
    }
}
