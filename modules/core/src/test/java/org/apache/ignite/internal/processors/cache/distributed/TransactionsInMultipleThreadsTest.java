/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.junit.Assert;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class TransactionsInMultipleThreadsTest extends GridCacheAbstractSelfTest {

    private TransactionConcurrency transactionConcurrency;

    private TransactionIsolation transactionIsolation;


    @Override
    protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
        stopAllGrids();
    }

    @Override
    protected int gridCount() {
        return 2;
    }

    @Override
    protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    @Override
    protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    public Transaction startTransaction(IgniteTransactions transactions) {
        return transactions.txStart(transactionConcurrency, transactionIsolation);
    }

    public void withAllIsolationsAndConcurrencies(Consumer consumer) {
        withAllIsolationsAndConcurrencies(consumer, null);
    }

    public void withAllIsolationsAndConcurrencies(Consumer consumer, Object arg) {
        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                this.transactionConcurrency = concurrency;
                this.transactionIsolation = isolation;
                try {
                    consumer.accept(arg);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void testSimpleTransactionInAnotherThread() throws IgniteCheckedException {
        withAllIsolationsAndConcurrencies(new Consumer() {
            @Override
            public void accept(Object o) {
                TransactionsInMultipleThreadsTest.this.simpleTransactionInAnotherThread();
            }
        });
    }

    private void simpleTransactionInAnotherThread() {

        Ignite ignite1 = ignite(0);

        IgniteTransactions transactions = ignite1.transactions();

        IgniteCache<String, Integer> cache = ignite1.getOrCreateCache("testCache");

        Transaction tx = startTransaction(transactions);

        cache.put("key1", 1);

        cache.put("key2", 2);

        tx.stop();

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                Assert.assertNull(transactions.tx());
                Assert.assertEquals(TransactionState.STOPPED, tx.state());
                transactions.txStart(tx);
                Assert.assertEquals(TransactionState.ACTIVE, tx.state());
                cache.put("key3", 3);
                cache.remove("key2");
                tx.commit();
                return true;
            }
        });

        Assert.assertNull(fut.error());
        try {
            fut.get();
        } catch (IgniteCheckedException e) {
            fail("transaction in another thread failed");
        }

        Assert.assertEquals(TransactionState.COMMITTED, tx.state());
        Assert.assertEquals((long) 1, (long) cache.get("key1"));
        Assert.assertEquals((long) 3, (long) cache.get("key3"));
        Assert.assertFalse(cache.containsKey("key2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCacheTransactionInAnotherThread() throws Exception {

        withAllIsolationsAndConcurrencies(new Consumer() {
            @Override
            public void accept(Object o) {
                TransactionsInMultipleThreadsTest.this.crossCacheTransactionInAnotherThread();
            }
        });

    }

    private void crossCacheTransactionInAnotherThread() {

        Ignite ignite1 = ignite(0);

        IgniteTransactions transactions = ignite1.transactions();

        IgniteCache<String, Integer> cache = ignite1.getOrCreateCache("testCache");
        IgniteCache<String, Integer> cache2 = ignite1.getOrCreateCache("testCache2");

        Transaction tx = startTransaction(transactions);

        cache.put("key1", 1);

        cache2.put("key2", 2);

        tx.stop();

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                Assert.assertNull(transactions.tx());
                Assert.assertEquals(TransactionState.STOPPED, tx.state());
                transactions.txStart(tx);
                Assert.assertEquals(TransactionState.ACTIVE, tx.state());
                cache.put("key3", 3);
                cache2.remove("key2");
                tx.commit();
                return true;
            }
        });

        Assert.assertNull(fut.error());
        try {
            fut.get();
        } catch (IgniteCheckedException e) {
            fail("transaction in another thread failed");
        }

        Assert.assertEquals(TransactionState.COMMITTED, tx.state());
        Assert.assertEquals((long) 1, (long) cache.get("key1"));
        Assert.assertEquals((long) 3, (long) cache.get("key3"));
        Assert.assertFalse(cache2.containsKey("key2"));
    }
}
