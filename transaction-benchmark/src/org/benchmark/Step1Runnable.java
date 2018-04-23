package org.benchmark;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.lang.GridTuple6;
import org.apache.ignite.transactions.Transaction;
import org.apache.log4j.Logger;

import static org.apache.ignite.transactions.TransactionState.SUSPENDED;

/*
 * Suspend-resume scenario first step.
 */
public class Step1Runnable implements Runnable {
    /** Ignite. */
    private final Ignite ignite;
    /** Test config. */
    private final TestConfiguration testCfg;
    /** Logger. */
    private final Logger log;
    /** Output queue. */
    private BlockingQueue<GridTuple6<Transaction, Integer, Long, Long, Long, Long>> outputQueue;
    /** Cache. */
    private IgniteCache<Integer, CacheValueHolder> cache;
    /** Origin key. */
    private int originKey;
    /** Keys numb per group. */
    private int keysNumbPerGrp;

    /**
     * @param outputQueue Output queue.
     * @param cache Cache.
     * @param log Logger.
     * @param ignite Ignite.
     * @param testCfg Test config.
     * @param originKey originKey.
     * @param keysNumbPerGrp keys number per group.
     */
    public Step1Runnable(
        BlockingQueue<GridTuple6<Transaction, Integer, Long, Long, Long, Long>> outputQueue,
        IgniteCache<Integer, CacheValueHolder> cache,
        Logger log,
        Ignite ignite,
        TestConfiguration testCfg,
        int originKey,
        int keysNumbPerGrp) {

        this.outputQueue = outputQueue;
        this.cache = cache;
        this.ignite = ignite;
        this.testCfg = testCfg;
        this.log = log;
        this.originKey = originKey;
        this.keysNumbPerGrp = keysNumbPerGrp;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        Transaction tx = null;

        try {
            while (true) {
                int key = ThreadLocalRandom.current().nextInt(originKey, originKey + keysNumbPerGrp);

                long totalTime = System.nanoTime();

                tx = ignite.transactions().txStart(testCfg.txConcurrency, testCfg.txIsolation);

                long txStartTime = System.nanoTime() - totalTime;

                CacheValueHolder val = cache.get(key);

                val.val += 2;

                cache.put(key, val);

                long txSuspendTime = System.nanoTime();

                tx.suspend();

                txSuspendTime = System.nanoTime() - txSuspendTime;

                //tx, key, start-time, suspend-time, resume-time, total-time
                outputQueue.put(new GridTuple6(tx, key, txStartTime, txSuspendTime, 0, System.nanoTime() - totalTime));
            }
        }
        catch (Throwable t) {
            if (!(t instanceof InterruptedException))
                log.error("Exception while transaction is in progress", t);
        }
        finally {
            if (tx != null) {
                if (SUSPENDED.equals(tx.state()))
                    tx.resume();

                tx.close();
            }
        }
    }
}
