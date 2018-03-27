package org.apache.ignite.examples.transactions;

import java.util.Arrays;
import java.util.HashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/*
 */
public class SuspendResumeUsageExample {

    /*
     */
    private static class TestObject {
        /** Test field. */
        int testField;

        /**
         * @param testField Test field.
         */
        TestObject(int testField) {
            this.testField = testField;
        }
    }

    /** Tx storage. */
    private static HashMap<Integer, Transaction> txStorage = new HashMap<>();

    /**
     * Simulates akka call from another thread.
     * Every akka call must hold unique transaction identifier in order to extract it from tx storage and resume.
     *
     * @param runnable Runnable.
     */
    private static void akkaCall(Runnable runnable) throws InterruptedException {
        Thread th = new Thread(runnable);

        th.start();

        th.join();
    }

    /**
     * Perform some logic in transactions and suspend\resume between them.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws InterruptedException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {

            IgniteCache<Integer, TestObject> cache = createCache(ignite);

            akkaCall(() -> {
                    Transaction tx = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);

                    txStorage.put(1, tx);

                    cache.put(1, new TestObject(1));

                    tx.suspend();
                }
            );

            akkaCall(() -> {
                    Transaction tx2 = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);

                    txStorage.put(2, tx2);

                    cache.put(2, new TestObject(2));

                    tx2.suspend();
                }
            );

            akkaCall(() -> {
                    Transaction tx3 = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);

                    txStorage.put(3, tx3);

                    cache.put(3, new TestObject(3));

                    tx3.suspend();
                }
            );

            akkaCall(() -> {
                    Transaction tx = txStorage.get(1);

                    tx.resume();

                    tx.commit();
                }
            );

            akkaCall(() -> {
                    Transaction tx = txStorage.get(2);

                    tx.resume();

                    TestObject testObject = cache.get(1);

                    cache.put(2, testObject);

                    tx.suspend();
                }
            );

            akkaCall(() -> {
                    Transaction tx = txStorage.get(3);

                    tx.resume();

                    cache.remove(3);

                    cache.put(4, new TestObject(4));

                    tx.commit();
                }
            );

            akkaCall(() -> {
                    Transaction tx = txStorage.get(2);

                    tx.resume();

                    tx.commit();
                }
            );
        }
    }


    /**
     * @param ignite Ignite.
     */
    private static IgniteCache<Integer, TestObject> createCache(Ignite ignite) {
        CacheConfiguration<Integer, TestObject> cacheCfg = new CacheConfiguration<>("cacheName");

        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        return ignite.getOrCreateCache(cacheCfg);
    }
}
