package org.benchmark;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.transactions.Transaction;

/**
 * Task that we need to perform in other thread as fourth step in test scenario.
 */
public class FourthStepTask implements Runnable{
    /** Cache. */
    private final IgniteCache<Integer, CacheValueHolder> cache;
    /** Key. */
    private final Integer key;
    /** Tx. */
    private final Transaction tx;
    /** Pool. */
    private SimpleSingleThreadPool pool;

    /**
     * @param pool Pool.
     * @param tx
     * @param cache
     * @param key
     */
    public FourthStepTask(SimpleSingleThreadPool pool, Transaction tx,
        IgniteCache<Integer, CacheValueHolder> cache, Integer key) {
        this.pool = pool;
        this.cache = cache;
        this.key = key;
        this.tx = tx;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        tx.resume();

        pool.timeVar1 = System.nanoTime();

        CacheValueHolder val = cache.get(key);

        val.val /= 4;

        cache.put(key, val);

        long time = System.nanoTime();

        tx.commit();

        pool.timeVar2 = System.nanoTime() - time;
    }
}
