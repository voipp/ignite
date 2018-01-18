package org.apache.ignite.spi.communication.tcp;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

public class IgniteMultipleOptimisticTxCreationRunnable implements IgniteRunnable {
    public Integer firstKey;

    public Integer secondKey;

    @IgniteInstanceResource
    private Ignite ignite;

    @Override public void run() {
        IgniteCache<Object, Object> cache = ignite.cache("default");

        UUID nodeId = ignite.cluster().localNode().id();

        System.out.println("[txs]Before starting tx on " + nodeId);

        Transaction tx = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);

        System.out.println("[txs]Before putting first " + firstKey + " on " + nodeId);

        cache.put(firstKey, firstKey);

        System.out.println("[txs]Before putting second " + firstKey + " on " + nodeId);

        cache.put(secondKey, secondKey);

        System.out.println("[txs]Before committing on " + nodeId);

        tx.commit();
    }
}
