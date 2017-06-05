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

import org.apache.ignite.*;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.junit.Assert;

import java.util.function.Consumer;

public class TransactionsInMultipleThreads extends GridCacheAbstractSelfTest {

    private TransactionConcurrency transactionConcurrency;

    private TransactionIsolation transactionIsolation;

    @Override
    protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
        stopAllGrids();

    }

    @Override
    protected int gridCount() {
        return 1;
    }

    @Override
    protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    @Override
    protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    @Override
    protected void beforeTest() throws Exception {
        super.beforeTest();
//        TestCommunicationSpi.sendingMustFailed = false;
    }

    private void withAllIsolationsAndConcurrencies(Consumer consumer) {
        withAllIsolationsAndConcurrencies(consumer, null);
    }

    private void withAllIsolationsAndConcurrencies(Consumer consumer, Object arg) {
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
                try {
                    TransactionsInMultipleThreads.this.simpleTransactionInAnotherThread();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void simpleTransactionInAnotherThread() throws Exception {
        IgniteCache<String, Integer> cache = jcache(grid(0), cacheConfiguration(null).setName("testCache"), String.class, Integer.class);
        IgniteTransactions transactions = transactions();

        Transaction tx = transactions.txStart(transactionConcurrency, transactionIsolation);
        cache.put("key1", 1);
        cache.put("key2", 2);
        tx.stop();

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> {
            Assert.assertNull(transactions.tx());
            Assert.assertEquals(TransactionState.STOPPED, tx.state());
            tx.resume();
            Assert.assertEquals(TransactionState.ACTIVE, tx.state());
            cache.put("key3", 3);
            cache.remove("key2");
            tx.commit();
            return true;
        });

        Assert.assertNull(fut.error());
        try {
            fut.get();
        } catch (IgniteCheckedException e) {
            fail("transaction in another thread failed.Concurrency control=" + transactionConcurrency +
            ". Isolation level=" + transactionIsolation);
        }

        Assert.assertEquals(TransactionState.COMMITTED, tx.state());
        Assert.assertEquals((long) 1, (long) cache.get("key1"));
        Assert.assertEquals((long) 3, (long) cache.get("key3"));
        Assert.assertFalse(cache.containsKey("key2"));
    }

    public void testSimpleTransactionInAnotherThreadContinued() throws IgniteCheckedException {
        withAllIsolationsAndConcurrencies(new Consumer() {
            @Override
            public void accept(Object o) {
                try {
                    TransactionsInMultipleThreads.this.simpleTransactionInAnotherThreadContinued();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void simpleTransactionInAnotherThreadContinued() throws Exception {
        IgniteCache<String, Integer> cache = jcache(grid(0), cacheConfiguration(null).setName("testCache"), String.class, Integer.class);
        IgniteTransactions transactions = transactions();

        Transaction tx = transactions.txStart(transactionConcurrency, transactionIsolation);
        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key1'", 1);
        tx.stop();

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> {
            Assert.assertNull(transactions.tx());
            Assert.assertEquals(TransactionState.STOPPED, tx.state());
            tx.resume();
            Assert.assertEquals(TransactionState.ACTIVE, tx.state());
            cache.put("key3", 3);
            cache.put("key2'", 2);
            cache.remove("key2", 2);
            tx.stop();
            return true;
        });
        try {
            fut.get();
        } catch (IgniteCheckedException e) {
            fail("transaction in another thread failed.Concurrency control=" + transactionConcurrency +
                    ". Isolation level=" + transactionIsolation);
        }

        Assert.assertNull(transactions.tx());
        Assert.assertEquals(TransactionState.STOPPED, tx.state());
        tx.resume();
        Assert.assertEquals(TransactionState.ACTIVE, tx.state());
        cache.remove("key1'", 1);
        cache.remove("key2'", 2);
        cache.put("key3'", 3);

        tx.commit();

        Assert.assertEquals(TransactionState.COMMITTED, tx.state());
        Assert.assertEquals((long) 1, (long) cache.get("key1"));
        Assert.assertEquals((long) 3, (long) cache.get("key3"));
        Assert.assertEquals((long) 3, (long) cache.get("key3'"));
        Assert.assertFalse(cache.containsKey("key2"));
        Assert.assertFalse(cache.containsKey("key1'"));
        Assert.assertFalse(cache.containsKey("key2'"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCacheTransactionInAnotherThread() throws Exception {
        withAllIsolationsAndConcurrencies(new Consumer() {
            @Override
            public void accept(Object o) {
                try {
                    TransactionsInMultipleThreads.this.crossCacheTransactionInAnotherThread();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void crossCacheTransactionInAnotherThread() throws Exception {
        Ignite ignite1 = ignite(0);

        IgniteTransactions transactions = ignite1.transactions();

        IgniteCache<String, Integer> cache = ignite1.getOrCreateCache(cacheConfiguration(null).setName("testCache"));
        IgniteCache<String, Integer> cache2 = ignite1.getOrCreateCache(cacheConfiguration(null).setName("testCache2"));

        Transaction tx = transactions.txStart(transactionConcurrency, transactionIsolation);
        cache.put("key1", 1);
        cache2.put("key2", 2);
        tx.stop();

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> {
            Assert.assertNull(transactions.tx());
            Assert.assertEquals(TransactionState.STOPPED, tx.state());
            tx.resume();
            Assert.assertEquals(TransactionState.ACTIVE, tx.state());
            cache.put("key3", 3);
            cache2.remove("key2");
            tx.commit();
            return true;
        });

        Assert.assertNull(fut.error());
        try {
            fut.get();
        } catch (IgniteCheckedException e) {
            fail("transaction in another thread failed.Concurrency control=" + transactionConcurrency +
                    ". Isolation level=" + transactionIsolation);
        }

        Assert.assertEquals(TransactionState.COMMITTED, tx.state());
        Assert.assertEquals((long) 1, (long) cache.get("key1"));
        Assert.assertEquals((long) 3, (long) cache.get("key3"));
        Assert.assertFalse(cache2.containsKey("key2"));
    }

    public void testCrossCacheTransactionInAnotherThreadContinued() throws Exception {
        withAllIsolationsAndConcurrencies(new Consumer() {
            @Override
            public void accept(Object o) {
                try {
                    TransactionsInMultipleThreads.this.crossCacheTransactionInAnotherThreadContinued();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void crossCacheTransactionInAnotherThreadContinued() throws Exception {
        Ignite ignite1 = ignite(0);

        IgniteTransactions transactions = ignite1.transactions();

        IgniteCache<String, Integer> cache = ignite1.getOrCreateCache(cacheConfiguration(null).setName("testCache"));
        IgniteCache<String, Integer> cache2 = ignite1.getOrCreateCache(cacheConfiguration(null).setName("testCache2"));

        Transaction tx = transactions.txStart(transactionConcurrency, transactionIsolation);
        cache.put("key1", 1);
        cache2.put("key2", 2);
        cache.put("key1'", 1);
        tx.stop();

        System.out.println("[TRANSACTION] thread id=" + Thread.currentThread().getId());
        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> {
            Assert.assertNull(transactions.tx());
            Assert.assertEquals(TransactionState.STOPPED, tx.state());
            tx.resume();
            Assert.assertEquals(TransactionState.ACTIVE, tx.state());
            cache.put("key3", 3);
            cache2.put("key2'", 2);
            cache2.remove("key2");
            tx.stop();
            return true;
        });

        try {
            fut.get();
        } catch (IgniteCheckedException e) {
            fail("transaction in another thread failed.Concurrency control=" + transactionConcurrency +
                    ". Isolation level=" + transactionIsolation);
        }

        Assert.assertNull(transactions.tx());
        Assert.assertEquals(TransactionState.STOPPED, tx.state());
        tx.resume();
        Assert.assertEquals(TransactionState.ACTIVE, tx.state());
        cache.remove("key1'", 1);
        cache2.remove("key2'", 2);
        cache.put("key3'", 3);

        tx.commit();

        Assert.assertEquals(TransactionState.COMMITTED, tx.state());
        Assert.assertEquals((long) 1, (long) cache.get("key1"));
        Assert.assertEquals((long) 3, (long) cache.get("key3"));
        Assert.assertEquals((long) 3, (long) cache.get("key3'"));
        Assert.assertFalse(cache2.containsKey("key2"));
        Assert.assertFalse(cache2.containsKey("key2'"));
        Assert.assertFalse(cache.containsKey("key1'"));
    }

    public void testTransactionRollback() throws InterruptedException, IgniteCheckedException {
        withAllIsolationsAndConcurrencies(new Consumer() {
            @Override
            public void accept(Object o) {
                try {
                    TransactionsInMultipleThreads.this.transactionRollback();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void transactionRollback() throws Exception {

        IgniteCache<String, Integer> cache1 = ignite(0).getOrCreateCache(cacheConfiguration(null).setName("testCache"));

        Transaction tx = transactions().txStart();
        cache1.put("key1", 1);
        cache1.put("key2", 2);
        tx.stop();

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> {
            Assert.assertNull(transactions().tx());
            Assert.assertEquals(TransactionState.STOPPED, tx.state());
            tx.resume();
            Assert.assertEquals(TransactionState.ACTIVE, tx.state());
            cache1.put("key3", 3);
            Assert.assertTrue(cache1.remove("key2"));
            tx.rollback();
            return true;
        });

        try {
            GridTestUtils.waitForCondition(() -> fut.isDone(), 5000L);
        } catch (IgniteInterruptedCheckedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(TransactionState.ROLLED_BACK, tx.state());
        Assert.assertFalse(cache1.containsKey("key1"));
        Assert.assertFalse(cache1.containsKey("key2"));
        Assert.assertFalse(cache1.containsKey("key3"));
    }
}
