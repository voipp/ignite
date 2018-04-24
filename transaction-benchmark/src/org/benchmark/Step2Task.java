package org.benchmark;

import java.util.concurrent.BlockingQueue;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.lang.GridTuple6;
import org.apache.ignite.transactions.Transaction;
import org.apache.log4j.Logger;

import static org.apache.ignite.transactions.TransactionState.SUSPENDED;

/*
 * Suspend-resume scenario second step.
 */
public class Step2Task extends Task {
    /** Input queue. */
    private BlockingQueue<GridTuple6<Transaction, Integer, Long, Long, Long, Long>> inputQueue;
    /** Output queue. */
    private BlockingQueue<GridTuple6<Transaction, Integer, Long, Long, Long, Long>> outputQueue;
    /** Cache. */
    private IgniteCache<Integer, CacheValueHolder> cache;

    /**
     * @param inputQueue Input queue.
     * @param outputQueue Output queue.
     * @param cache Cache.
     * @param log Logger.
     */
    public Step2Task(
        BlockingQueue<GridTuple6<Transaction, Integer, Long, Long, Long, Long>> inputQueue,
        BlockingQueue<GridTuple6<Transaction, Integer, Long, Long, Long, Long>> outputQueue,
        IgniteCache<Integer, CacheValueHolder> cache,
        Logger log) {
        super(log);

        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;
        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        Transaction tx = null;

        try {
            while (active) {
                //tx, key, start-time, suspend-time, resume-time, total-time
                GridTuple6<Transaction, Integer, Long, Long, Long, Long> inputData = inputQueue.take();

                tx = inputData.get1();

                long totalTime = System.nanoTime();

                tx.resume();

                long txResumeTime = System.nanoTime() - totalTime;

                CacheValueHolder val = cache.get(inputData.get2());

                val.val /= 2;

                cache.put(inputData.get2(), val);

                long txSuspendTime = System.nanoTime();

                tx.suspend();

                txSuspendTime = System.nanoTime() - txSuspendTime;

                inputData.set6(System.nanoTime() - totalTime + inputData.get6());//total time
                inputData.set5(txResumeTime);//resume time
                inputData.set4(txSuspendTime + inputData.get4());//suspend time

                //tx, key, start-time, suspend-time, resume-time, total-time
                outputQueue.put(inputData);

                tx = null;
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
