package org.benchmark;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.transactions.Transaction;

/**
 * Created by SBT-Kuznetsov-AL on 11.04.2018.
 */
public class SimpleSingleThreadPool {
    /** key. */
    Integer key;

    /** Task to execute. */
    private volatile Runnable task;

    /** Preconstructed task instance. */
    private volatile Runnable task2;

    /** Preconstructed task instance. */
    private volatile Runnable task4;

    /** Time variable. */
    volatile long timeVar1;

    /** Time variable. */
    volatile long timeVar2;

    /** Tx. */
    Transaction tx;

    /** Cache. */
    IgniteCache<Integer, CacheValueHolder> cache;

    /** Stop flag. */
    private volatile boolean work = true;

    /**
     * Default constructor.
     *
     * @param cache Cache.
     */
    public SimpleSingleThreadPool(IgniteCache<Integer, CacheValueHolder> cache) {
        this.cache = cache;

        ThreadPoolWorker worker = new ThreadPoolWorker();

        worker.setDaemon(true);

        worker.start();

        task2 = new SecondStepTask(this, tx, cache, key);
        task4 = new FourthStepTask(this, tx, cache, key);
    }

    /**
     *
     */
    public void stopWorker() {
        work = false;
    }

    /**
     * Submitting 1 preconfigured task to executor.
     */
    public void executeSecondStepTask(Transaction tx, Integer key) {
        this.tx = tx;
        this.key = key;

        this.task = task2;

        while (this.task != null)
            Thread.yield();
    }

    /**
     * Submitting 2 preconfigured task to executor.
     */
    public void executeFourthStepTask(Transaction tx, Integer key) {
        this.tx = tx;
        this.key = key;

        this.task = task4;

        while (this.task != null)
            Thread.yield();
    }

    /** */
    private class ThreadPoolWorker extends Thread {
        /** {@inheritDoc} */
        @Override public void run() {
            while (work) {
                while (task != null) {
                    task.run();

                    task = null;
                }

                Thread.yield();
            }
        }
    }
}
