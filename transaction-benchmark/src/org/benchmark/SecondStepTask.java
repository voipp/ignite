package org.benchmark;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.transactions.Transaction;

/**
 * Task that we need to perform in other thread as second step in test scenario.
 */
public class SecondStepTask implements Runnable {
    /** Tx. */
    private Transaction tx;
    /** Cache. */
    private IgniteCache<Integer, CacheValueHolder> cache;
    /** Key. */
    private Integer key;
    /** Pool. */
    private SimpleSingleThreadPool pool;
    /**
     * @param pool Pool.
     * @param tx
     * @param cache
     * @param key
     */
    public SecondStepTask(SimpleSingleThreadPool pool, Transaction tx,
        IgniteCache<Integer, CacheValueHolder> cache, Integer key) {
        this.pool = pool;
        this.tx = tx;
        this.cache = cache;
        this.key = key;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        tx.resume();

        pool.timeVar1 = System.nanoTime();

        CacheValueHolder val = cache.get(key);

        val.val /= 2;

        cache.put(key, val);

        pool.timeVar2 = System.nanoTime();

        tx.suspend();
    }
}
