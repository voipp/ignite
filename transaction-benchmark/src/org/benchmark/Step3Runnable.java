package org.benchmark;

import java.util.concurrent.BlockingQueue;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.transactions.Transaction;
import org.apache.log4j.Logger;

/*
 * Suspend-resume scenario third step.
 */
public class Step3Runnable implements Runnable {
    /** Logger. */
    private final Logger log;
    /** Input queue. */
    private BlockingQueue<GridTuple3<Transaction, Integer, Long>> inputQueue;
    /** Output queue. */
    private BlockingQueue<GridTuple3<Transaction, Integer, Long>> outputQueue;
    /** Cache. */
    private IgniteCache<Integer, CacheValueHolder> cache;

    /**
     * @param inputQueue Input queue.
     * @param outputQueue Output queue.
     * @param cache Cache.
     * @param log Logger.
     */
    public Step3Runnable(
        BlockingQueue<GridTuple3<Transaction, Integer, Long>> inputQueue,
        BlockingQueue<GridTuple3<Transaction, Integer, Long>> outputQueue,
        IgniteCache<Integer, CacheValueHolder> cache,
        Logger log) {

        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;
        this.cache = cache;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        Transaction tx = null;
        GridTuple3<Transaction, Integer, Long> outputData = new GridTuple3<>();

        try {
            while (true) {
                GridTuple3<Transaction, Integer, Long> inputData = inputQueue.take();

                tx = inputData.get1();

                long totalTime = System.nanoTime();

                tx.resume();

                long txResumeTime = System.nanoTime() - totalTime;

                CacheValueHolder val = cache.get(inputData.get2());

                val.val += 2;

                cache.put(inputData.get2(), val);

                long txSuspendTime = System.nanoTime();

                tx.suspend();

                txSuspendTime = System.nanoTime() - txSuspendTime;

                outputData.set(tx, inputData.get2(), System.nanoTime() - totalTime + inputData.get3());

                log.debug(
                    "TxSuspendTime=" + txSuspendTime
                        + "\nTxResumeTime=" + txResumeTime
                );

                outputQueue.add(outputData);
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
