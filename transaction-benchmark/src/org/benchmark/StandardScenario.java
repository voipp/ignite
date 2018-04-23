package org.benchmark;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.transactions.Transaction;
import org.apache.log4j.Logger;

/*
 * Standard scenario.
 */
public class StandardScenario implements Runnable {
    /** Keys numb per group. */
    private int origin;
    /** Test config. */
    private TestConfiguration testCfg;
    /** Cache. */
    private IgniteCache<Integer, CacheValueHolder> cache;
    /** Logger. */
    private Logger log;
    /** Ignite. */
    private Ignite ignite;

    /**
     * @param origin Origin.
     * @param cfg Config.
     * @param cache Cache.
     * @param log Logger.
     * @param ignite Ignite.
     */
    public StandardScenario(
        int origin,
        TestConfiguration cfg,
        IgniteCache<Integer, CacheValueHolder> cache,
        Logger log,
        Ignite ignite) {

        this.origin = origin;
        this.testCfg = cfg;
        this.cache = cache;
        this.log = log;
        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        Transaction tx = null;

        try {
            while (true) {
                Integer key = ThreadLocalRandom.current().nextInt(origin, origin + testCfg.keysNumbPerGrp);

                long txTotalTime = System.nanoTime();

                tx = ignite.transactions().txStart(testCfg.txConcurrency, testCfg.txIsolation);

                long txStartTime = System.nanoTime() - txTotalTime;

                CacheValueHolder val = cache.get(key);

                val.val += 2;

                cache.put(key, val);

                val = cache.get(key);

                val.val /= 2;

                cache.put(key, val);

                val = cache.get(key);

                val.val += 3;

                cache.put(key, val);

                val = cache.get(key);

                val.val /= 4;

                cache.put(key, val);

                long txCommitTime = System.nanoTime();

                tx.commit();

                log.debug(
                        "\nTxTotalTime=" + (System.nanoTime() - txTotalTime) +
                        "\nTxStartTime=" + txStartTime +
                        "\nTxCommitTime=" + (System.nanoTime() - txCommitTime)
                );
            }
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }
}
