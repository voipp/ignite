package org.apache.ignite.mxbean;

import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/**
 * MBean that provides access to information about stripped executor service.
 */
@MXBeanDescription("MBean that provides access to information about stripped executor service.")
public interface StrippedExecutorMXBean {
    /**
     * Checks starvation in striped pool. Maybe too verbose
     * but this is needed to faster debug possible issues.
     */
    @MXBeanDescription("Starvation in striped pool.")
    public void checkStarvation();

    /**
     * @return Stripes count.
     */
    @MXBeanDescription("Stripes count.")
    public int getStripesCount();

    /**
     *
     * @return {@code True} if this executor has been shut down.
     */
    @MXBeanDescription("True if this executor has been shut down.")
    public boolean isShutdown();

    /**
     * Note that
     * {@code isTerminated()} is never {@code true} unless either {@code shutdown()} or
     * {@code shutdownNow()} was called first.
     *
     * @return {@code True} if all tasks have completed following shut down.
     */
    @MXBeanDescription("True if all tasks have completed following shut down.")
    public boolean isTerminated();

    /**
     * @return Return total queue size of all stripes.
     */
    @MXBeanDescription("Total queue size of all stripes.")
    public int getTotalQueueSize();

    /**
     * @return Completed tasks count.
     */
    @MXBeanDescription("Completed tasks count.")
    public long getTotalCompletedTasksCount();

    /**
     * @return Number of completed tasks per stripe.
     */
    @MXBeanDescription("Number of completed tasks per stripe.")
    public long[] getStripesCompletedTasksCount();

    /**
     * @return Number of active tasks.
     */
    @MXBeanDescription("Number of active tasks.")
    public int getActiveCount();

    /**
     * @return Number of active tasks per stripe.
     */
    @MXBeanDescription("Number of active tasks per stripe.")
    public boolean[] getStripesActiveStatuses();

    /**
     * @return Size of queue per stripe.
     */
    @MXBeanDescription("Size of queue per stripe.")
    public int[] getStripesQueueSizes();


    /**
     * {@inheritDoc}
     */
    public String toString();
}
