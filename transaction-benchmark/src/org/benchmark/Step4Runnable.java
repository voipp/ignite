package org.benchmark;

import java.util.concurrent.BlockingQueue;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.transactions.Transaction;
import org.apache.log4j.Logger;

/*
 * Suspend-resume scenario fourth step.
 */
public class Step4Runnable implements Runnable {
    /** Logger. */
    private final Logger log;
    /** Input queue. */
    private BlockingQueue<GridTuple3<Transaction, Integer, Long>> inputQueue;
    /** Cache. */
    private IgniteCache<Integer, CacheValueHolder> cache;

    /**
     * @param inputQueue Input queue.
     * @param cache Cache.
     * @param log Logger.
     */
    public Step4Runnable(
        BlockingQueue<GridTuple3<Transaction, Integer, Long>> inputQueue,
        IgniteCache<Integer, CacheValueHolder> cache,
        Logger log
    ) {

        this.inputQueue = inputQueue;
        this.cache = cache;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        Transaction tx = null;

        try {
            while (true) {
                GridTuple3<Transaction, Integer, Long> inputData = inputQueue.take();

                tx = inputData.get1();

                long totalTime = System.nanoTime();

                tx.resume();

                long txResumeTime = System.nanoTime() - totalTime;

                CacheValueHolder val = cache.get(inputData.get2());

                val.val /= 4;

                cache.put(inputData.get2(), val);

                long txCommitTime = System.nanoTime();

                tx.commit();

                txCommitTime = System.nanoTime() - txCommitTime;

                totalTime = System.nanoTime() - totalTime + inputData.get3();

                log.debug(
                    "TxCommitTime=" + txCommitTime
                        + "\nTxResumeTime=" + txResumeTime
                        + "\nTxTotalTime=" + totalTime
                );
            }
        }
        catch (InterruptedException e) {
            //No-op.
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }
}
