package org.apache.ignite.spi.communication.tcp;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

public class IgniteTxCreationRunnable implements IgniteRunnable {
    public Integer firstKey;

    public Integer secondKey;

    public UUID nodeId;

    @IgniteInstanceResource
    private Ignite ignite;

    @Override public void run() {
        if (ignite.cluster().localNode().id().equals(nodeId)) {
            IgniteCache<Object, Object> cache = ignite.cache("default");

            System.out.println("[txs]Before starting tx on " + nodeId);

            Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);

            System.out.println("[txs]Before putting first " + firstKey + " on " + nodeId);

            cache.put(firstKey, firstKey);

            System.out.println("[txs]Before putting second " + firstKey + " on " + nodeId);

            cache.putAsync(secondKey, firstKey);

            System.out.println("[txs]Before committing on " + nodeId);

            tx.commitAsync();
        }
    }
}
