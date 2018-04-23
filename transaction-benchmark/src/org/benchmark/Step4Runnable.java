package org.benchmark;

import java.util.concurrent.BlockingQueue;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.lang.GridTuple6;
import org.apache.ignite.transactions.Transaction;
import org.apache.log4j.Logger;

import static org.apache.ignite.transactions.TransactionState.*;

/*
 * Suspend-resume scenario fourth step.
 */
public class Step4Runnable implements Runnable {
    /** Logger. */
    private final Logger log;
    /** Input queue. */
    private BlockingQueue<GridTuple6<Transaction, Integer, Long, Long, Long, Long>> inputQueue;
    /** Cache. */
    private IgniteCache<Integer, CacheValueHolder> cache;

    /**
     * @param inputQueue Input queue.
     * @param cache Cache.
     * @param log Logger.
     */
    public Step4Runnable(
        BlockingQueue<GridTuple6<Transaction, Integer, Long, Long, Long, Long>> inputQueue,
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
                //tx, key, start-time, suspend-time, resume-time, total-time
                GridTuple6<Transaction, Integer, Long, Long, Long, Long> inputData = inputQueue.take();

                tx = inputData.get1();

                long totalTime = System.nanoTime();

                tx.resume();

                long txResumeTime = System.nanoTime() - totalTime + inputData.get5();

                CacheValueHolder val = cache.get(inputData.get2());

                val.val /= 4;

                cache.put(inputData.get2(), val);

                long txCommitTime = System.nanoTime();

                tx.commit();

                txCommitTime = System.nanoTime() - txCommitTime;

                totalTime = System.nanoTime() - totalTime + inputData.get6();

                log.debug(
                    inputData.get3() + // tx start time
                        "," + inputData.get4() / 3 + // tx suspend time
                        "," + txResumeTime / 3 + // tx resume time
                        "," + txCommitTime + // tx commit time
                        "," + totalTime // tx total time
                );
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
