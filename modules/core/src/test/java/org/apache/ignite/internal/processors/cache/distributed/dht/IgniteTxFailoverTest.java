package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.ComputeTaskInternalFuture;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.communication.tcp.BlockTcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.IgniteBlockingRunnable;
import org.apache.ignite.spi.communication.tcp.IgniteKamikazeRunnable;
import org.apache.ignite.spi.communication.tcp.IgniteMultipleOptimisticTxCreationRunnable;
import org.apache.ignite.spi.communication.tcp.IgniteReleasingRunnable;
import org.apache.ignite.spi.communication.tcp.IgniteTxCreationRunnable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.transactions.TransactionState;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.internal.util.typedef.X.hasCause;

/**
 *
 */
public class IgniteTxFailoverTest extends GridCommonAbstractTest {
    /** Client node. */
    private Ignite clientNode;
    /** Client. */
    private Ignite originatingNode;

    /** Primary node. */
    private ClusterNode primaryNode;

    /** Backup node 1. */
    private ClusterNode backupNode1;

    /** Backup node 2. */
    private ClusterNode backupNode2;

    /** Kamikaze runnable. */
    IgniteKamikazeRunnable kamikazeRunnable = new IgniteKamikazeRunnable();

    /** Blocking runnable. */
    IgniteBlockingRunnable blockingRunnable = new IgniteBlockingRunnable();

    /** Releasing runnable. */
    IgniteReleasingRunnable releasingRunnable = new IgniteReleasingRunnable();

    /**
     *
     */
    enum NODE_ROLE {
        /** Primary. */PRIMARY,

        /** Both backups. */BOTH_BACKUPS,

        /** Only backup. */ONLY_BACKUP,

        /** Originating. */ORIGINATING
    }

    /**
     *
     */
    protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /**
     *
     */
    protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    /**
     *
     */
    protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration icfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = defaultCacheConfiguration();
        ccfg.setBackups(2);
        ccfg.setNearConfiguration(new NearCacheConfiguration());
        ccfg.setCacheMode(cacheMode());
        ccfg.setAtomicityMode(atomicityMode());
        icfg.setCacheConfiguration(ccfg);

        icfg.setCommunicationSpi(new BlockTcpCommunicationSpi());

        return icfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        try {
            originatingNode = G.start("C:\\work\\ignite\\bin\\config.xml");
            clientNode = G.start("C:\\work\\ignite\\bin\\client-config.xml");

            awaitPartitionMapExchange();

            IgniteCache cache = originatingNode.getOrCreateCache(getConfiguration().getCacheConfiguration()[0]);

            cache.removeAll();

            Collection<ClusterNode> remoteNodes = originatingNode.cluster().forRemotes().forServers().nodes();

            assertEquals(3, remoteNodes.size());

            Iterator<ClusterNode> iterator = remoteNodes.iterator();

            primaryNode = iterator.next();
            backupNode1 = iterator.next();
            backupNode2 = iterator.next();

            assert !iterator.hasNext();

            System.out.println("[txs]local node " + originatingNode.cluster().localNode().id());
            System.out.println("[txs]client node " + clientNode.cluster().localNode().id());
            System.out.println("[txs]primary node " + primaryNode.id());
            System.out.println("[txs]backup node " + backupNode1.id());
            System.out.println("[txs]backup node2 " + backupNode2.id());
        } catch (Throwable e){
            e.printStackTrace();

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
//        kamikazeRunnable.nodesToKill = null;
//
//        try {
//            originatingNode.compute(originatingNode.cluster().forRemotes()).broadcast(kamikazeRunnable);
//        }
//        catch (Throwable ignore) {
//            //No-op.
//        }
//
//        G.stop(true);

        G.stop(clientNode.name(), true);

        G.stop(true);
    }

    /**
     * @param failingNode Failing node.
     */
    private void stopNode(NODE_ROLE failingNode) {
        switch (failingNode) {
            case PRIMARY:/* primary */
                //assert G.stop(primaryNode.name(), true);

                kamikazeRunnable.nodesToKill = new UUID[]{primaryNode.id()};

                originatingNode.compute(originatingNode.cluster().forNodeId(primaryNode.id())).broadcast(kamikazeRunnable);

                break;
            case BOTH_BACKUPS:/* backups */
//                assert G.stop(backupNode1.name(), true);
//                assert G.stop(backupNode2.name(), true);
                kamikazeRunnable.nodesToKill = new UUID[]{backupNode1.id(), backupNode2.id()};

                originatingNode.compute(originatingNode.cluster().forNodeIds(Arrays.asList(backupNode1.id(), backupNode2.id()))).broadcast(kamikazeRunnable);

                break;
            case ONLY_BACKUP/* only one backup */ :
                //assert G.stop(backupNode1.name(), true);

                kamikazeRunnable.nodesToKill = new UUID[]{backupNode1.id()};

                originatingNode.compute(originatingNode.cluster().forNodeId(backupNode1.id())).broadcast(kamikazeRunnable);
                break;
            case ORIGINATING/* originating node */ :
                commSpi(originatingNode).simulateNodeFailure();

                break;
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    public void testPrimaryReceivedPrepareAndFailed() throws Exception {
        //key that would be primary on grid1, and backup on grid2, grid3
        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (originatingNode.affinity(DEFAULT_CACHE_NAME).isPrimary(primaryNode, i))
                if (originatingNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode1, i))
                    if (originatingNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode2, i)) {
                        key = i;

                        break;
                    }
        }

        assert key != null;

        IgniteCache<Object, Object> cache = originatingNode.cache(DEFAULT_CACHE_NAME);

        blockMessage(GridDhtTxPrepareRequest.class, false, primaryNode.id());

        Thread.sleep(1_000);

        try {
            System.out.println("[txs]before commiting");

            Integer finalKey = key;
            Ignite finalClient = originatingNode;
            // commit thread will be blocked until unblockMessage() is called
            IgniteInternalFuture<?> commitFut = multithreadedAsync(
                new Runnable() {
                    @Override public void run() {
                        Transaction tx0 = finalClient.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);

                        cache.put(finalKey, 1);

                        tx0.commit();

                        System.out.println(tx0.toString());
                    }
                }, 1);

            System.out.println("[txs]Transaction started. waiting 1 sec");

            Thread.sleep(1_000);

            assert !commitFut.isDone();
            System.out.println("[txs]Stopping node " + NODE_ROLE.PRIMARY);

            stopNode(NODE_ROLE.PRIMARY);

            System.out.println("[txs]waiting after killing node");

            Thread.sleep(1_000);

            assertNotNull(commitFut.toString(), commitFut.error());

            System.out.println("[txs]commit must be finished");

            commitFut.get();
        }
        catch (Throwable ignored) {
            //No-op.
            System.out.println("[txs]error occured " + ignored);
        }
        finally {

            assertNull(originatingNode.cache(DEFAULT_CACHE_NAME).get(key));

            System.out.println("[txs]Test passed!");
        }
    }

    public void testPrimaryReceivedFinishAndFailed() throws Exception {
        //key that would be primary on grid1, and backup on grid2, grid3
        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (originatingNode.affinity(DEFAULT_CACHE_NAME).isPrimary(primaryNode, i))
                if (originatingNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode1, i))
                    if (originatingNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode2, i)) {
                        key = i;

                        break;
                    }
        }

        assert key != null;

        IgniteCache<Object, Object> cache = originatingNode.cache(DEFAULT_CACHE_NAME);

        System.out.println("[txs]blocking message");

        blockMessage(GridDhtTxFinishRequest.class, false, primaryNode.id());

        try {
            System.out.println("[txs]before commiting");

            Integer finalKey = key;
            Ignite finalClient = originatingNode;
            // commit thread will be blocked until unblockMessage() is called
            IgniteInternalFuture<?> commitFut = multithreadedAsync(
                new Runnable() {
                    @Override public void run() {
                        Transaction tx0 = finalClient.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);

                        cache.put(finalKey, 1);

                        tx0.commit();

                        System.out.println(tx0.toString());
                    }
                }, 1);

            System.out.println("[txs]Transaction started. waiting 1 sec");

            assert !commitFut.isDone();

            System.out.println("[txs]Stopping node " + NODE_ROLE.PRIMARY);

            stopNode(NODE_ROLE.PRIMARY);

            System.out.println("[txs]waiting after killing node");

            Thread.sleep(1_000);

            assertNull(commitFut.error());

            System.out.println("[txs]commit must be finished");

            commitFut.get();
        }
        catch (Throwable ignored) {
            //No-op.
            assert false : "Transaction must be completed succesfully";
        }
        finally {

            assertNotNull(originatingNode.cache(DEFAULT_CACHE_NAME).get(key));

            System.out.println("[txs]Test passed!");
        }
    }

    public void testBackupsReceivedPrepareAndFailed() throws Exception {
        //key that would be primary on grid1, and backup on grid2, grid3
        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (originatingNode.affinity(DEFAULT_CACHE_NAME).isPrimary(primaryNode, i))
                if (originatingNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode1, i))
                    if (originatingNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode2, i)) {
                        key = i;

                        break;
                    }
        }

        assert key != null;

        IgniteCache<Object, Object> cache = originatingNode.cache(DEFAULT_CACHE_NAME);

        System.out.println("[txs]blocking message");

        blockMessage(GridDhtTxPrepareResponse.class, false, backupNode1.id(), backupNode2.id());

        Thread.sleep(1_000);

        try {
            System.out.println("[txs]before commiting");

            Integer finalKey = key;
            Ignite finalClient = originatingNode;
            // commit thread will be blocked until unblockMessage() is called
            IgniteInternalFuture<?> commitFut = multithreadedAsync(
                new Runnable() {
                    @Override public void run() {
                        Transaction tx0 = finalClient.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);

                        cache.put(finalKey, 1);

                        tx0.commit();

                        System.out.println(tx0.toString());
                    }
                }, 1);

            System.out.println("[txs]Transaction started. waiting 1 sec");

            Thread.sleep(1_000);

            assert !commitFut.isDone();
            System.out.println("[txs]Stopping node " + NODE_ROLE.ONLY_BACKUP);

            stopNode(NODE_ROLE.BOTH_BACKUPS);

            System.out.println("[txs]waiting after killing node");

            Thread.sleep(1_000);

            assertNull(commitFut.error().toString(), commitFut.error());

            System.out.println("[txs]commit must be finished");

            commitFut.get();
        }
        catch (Throwable ignored) {
            //No-op.
            System.out.println("[txs]error occured " + ignored);
        }
        finally {
            assertNotNull(originatingNode.cache(DEFAULT_CACHE_NAME).get(key));

            System.out.println("[txs]Test passed!");
        }
    }

    public void testBackupsReceivedPrepareAndFailedOnlyOne() throws Exception {
        //key that would be primary on grid1, and backup on grid2, grid3
        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (originatingNode.affinity(DEFAULT_CACHE_NAME).isPrimary(primaryNode, i))
                if (originatingNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode1, i))
                    if (originatingNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode2, i)) {
                        key = i;

                        break;
                    }
        }

        assert key != null;

        IgniteCache<Object, Object> cache = originatingNode.cache(DEFAULT_CACHE_NAME);

        System.out.println("[txs]blocking message");

        blockMessage(GridDhtTxPrepareResponse.class, false, backupNode1.id());
        blockMessage(GridDhtTxPrepareResponse.class, true, backupNode2.id());

        Thread.sleep(1_000);

        try {
            System.out.println("[txs]before commiting");

            Integer finalKey = key;
            Ignite finalClient = originatingNode;
            // commit thread will be blocked until unblockMessage() is called
            IgniteInternalFuture<?> commitFut = multithreadedAsync(
                new Runnable() {
                    @Override public void run() {
                        Transaction tx0 = finalClient.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);

                        cache.put(finalKey, 1);

                        tx0.commit();

                        System.out.println(tx0.toString());
                    }
                }, 1);

            System.out.println("[txs]Transaction started. waiting 1 sec");

            Thread.sleep(1_000);

            assert !commitFut.isDone();
            System.out.println("[txs]Stopping node " + NODE_ROLE.ONLY_BACKUP);

            stopNode(NODE_ROLE.ONLY_BACKUP);

            unblockMessages(backupNode2.id());

            System.out.println("[txs]waiting after killing node");

            Thread.sleep(1_000);

            assertNull(commitFut.error().toString(), commitFut.error());

            System.out.println("[txs]commit must be finished");

            commitFut.get();
        }
        catch (Throwable ignored) {
            //No-op.
            System.out.println("[txs]error occured " + ignored);
        }
        finally {
            assertNotNull(originatingNode.cache(DEFAULT_CACHE_NAME).get(key));

            System.out.println("[txs]Test passed!");
        }
    }

    public void testPrimaryReceivedPrepareAndCoordinatorFailedTxRollback() throws Exception {
        //key that would be primary on grid1, and backup on grid2, grid3
        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (originatingNode.affinity(DEFAULT_CACHE_NAME).isPrimary(primaryNode, i))
                if (originatingNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode1, i))
                    if (originatingNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode2, i)) {
                        key = i;

                        break;
                    }
        }

        assert key != null;

        IgniteCache<Object, Object> cache = originatingNode.cache(DEFAULT_CACHE_NAME);

        System.out.println("[txs]blocking message");

//        blockMessage(GridDhtTxPrepareRequest.class, true, primaryNode.id());
        blockMessage(GridDhtTxPrepareRequest.class, false, primaryNode.id());

        Thread.sleep(1_000);

        try {
            System.out.println("[txs]before commiting");

            Integer finalKey = key;
            Ignite finalClient = originatingNode;

            UUID originatingNodeId = originatingNode.cluster().localNode().id();

            // commit thread will be blocked until unblockMessage() is called
            IgniteInternalFuture<?> commitFut = multithreadedAsync(
                new Runnable() {
                    @Override public void run() {
                        Transaction tx0 = finalClient.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);

                        cache.put(finalKey, 1);

                        tx0.commit();

                        System.out.println("[txs]Transaction finished with status " + tx0.state());
                    }
                }, 1);

            System.out.println("[txs]Transaction started. waiting 1 sec");

            Thread.sleep(1_000);

            assert !commitFut.isDone();
            System.out.println("[txs]Stopping node " + NODE_ROLE.ORIGINATING);

            stopNode(NODE_ROLE.ORIGINATING);

            System.out.println("[txs]checking node has left cluster");

            for (ClusterNode node : clientNode.cluster().forServers().nodes())
                assertNotSame(node.id(), originatingNodeId);

            System.out.println("[txs]waiting after killing node");

            Thread.sleep(1_000);

            //unblockMessages(primaryNode.id());

            System.out.println("[txs]waiting after unblocking message on primary node");

            Thread.sleep(1_000);

            assertNull(commitFut.toString(), commitFut.error());

            System.out.println("[txs]commit must be finished");
        }
        catch (Throwable ignored) {
            //No-op.
            System.out.println("[txs]error occured " + ignored);
        }
        finally {
//            assertNotNull(clientNode.cache(DEFAULT_CACHE_NAME).get(key));
            assertNull(clientNode.cache(DEFAULT_CACHE_NAME).get(key));

            System.out.println("[txs]Test passed!");
        }
    }

    public void testPrimaryReceivedFinishAndCoordinatorFailedTxCommitted() throws Exception {
        //key that would be primary on grid1, and backup on grid2, grid3
        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (originatingNode.affinity(DEFAULT_CACHE_NAME).isPrimary(primaryNode, i))
                if (originatingNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode1, i))
                    if (originatingNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode2, i)) {
                        key = i;

                        break;
                    }
        }

        assert key != null;

        IgniteCache<Object, Object> cache = originatingNode.cache(DEFAULT_CACHE_NAME);

        System.out.println("[txs]blocking message");

        blockMessage(GridDhtTxFinishRequest.class, true, primaryNode.id());

        Thread.sleep(1_000);

        try {
            System.out.println("[txs]before commiting");

            Integer finalKey = key;
            Ignite finalClient = originatingNode;

            UUID originatingNodeId = originatingNode.cluster().localNode().id();

            // commit thread will be blocked until unblockMessage() is called
            IgniteInternalFuture<?> commitFut = multithreadedAsync(
                new Runnable() {
                    @Override public void run() {
                        Transaction tx0 = finalClient.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);

                        cache.put(finalKey, 1);

                        tx0.commit();

                        System.out.println("[txs]Transaction finished with status " + tx0.state());
                    }
                }, 1);

            System.out.println("[txs]Transaction started. waiting 1 sec");

            Thread.sleep(1_000);

            assert !commitFut.isDone();
            System.out.println("[txs]Stopping node " + NODE_ROLE.ORIGINATING);

            stopNode(NODE_ROLE.ORIGINATING);

            System.out.println("[txs]checking node has left cluster");

            for (ClusterNode node : clientNode.cluster().forServers().nodes())
                assertNotSame(node.id(), originatingNodeId);

            System.out.println("[txs]waiting after killing node");

            Thread.sleep(1_000);

            unblockMessages(primaryNode.id());

            System.out.println("[txs]waiting after unblocking message on primary node");

            Thread.sleep(1_000);

            assertNull(commitFut.toString(), commitFut.error());

            System.out.println("[txs]commit must be finished");
        }
        catch (Throwable ignored) {
            //No-op.
            System.out.println("[txs]error occured " + ignored);
        }
        finally {
            assertNotNull(clientNode.cache(DEFAULT_CACHE_NAME).get(key));

            System.out.println("[txs]Test passed!");
        }
    }

    public void testBackupsReceivedFinishAndFailed() throws Exception {
        //key that would be primary on grid1, and backup on grid2, grid3
        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (originatingNode.affinity(DEFAULT_CACHE_NAME).isPrimary(primaryNode, i))
                if (originatingNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode1, i))
                    if (originatingNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode2, i)) {
                        key = i;

                        break;
                    }
        }

        assert key != null;

        IgniteCache<Object, Object> cache = originatingNode.cache(DEFAULT_CACHE_NAME);

        System.out.println("[txs]blocking message");

        blockMessage(GridDhtTxFinishResponse.class, false, backupNode1.id(), backupNode2.id());

        Thread.sleep(1_000);

        try {
            System.out.println("[txs]before commiting");

            Integer finalKey = key;
            Ignite finalClient = originatingNode;
            // commit thread will be blocked until unblockMessage() is called
            IgniteInternalFuture<?> commitFut = multithreadedAsync(
                new Runnable() {
                    @Override public void run() {
                        Transaction tx0 = finalClient.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);

                        cache.put(finalKey, 1);

                        tx0.commit();

                        System.out.println(tx0.toString());
                    }
                }, 1);

            System.out.println("[txs]Transaction started. waiting 1 sec");

            Thread.sleep(1_000);

            assert !commitFut.isDone();
            System.out.println("[txs]Stopping node " + NODE_ROLE.ONLY_BACKUP);

            stopNode(NODE_ROLE.BOTH_BACKUPS);

            System.out.println("[txs]waiting after killing node");

            Thread.sleep(1_000);

            assertNull(commitFut.error().toString(), commitFut.error());

            System.out.println("[txs]commit must be finished");

            commitFut.get();
        }
        catch (Throwable ignored) {
            //No-op.
            System.out.println("[txs]error occured " + ignored);
        }
        finally {
            assertNotNull(originatingNode.cache(DEFAULT_CACHE_NAME).get(key));

            System.out.println("[txs]Test passed!");
        }
    }

    public void testBackupsReceivedFinishAndFailedOnlyOne() throws Exception {
        //key that would be primary on grid1, and backup on grid2, grid3
        Integer key;

        key = primaryKey(primaryNode, backupNode1, backupNode2);

        assert key != null;

        IgniteCache<Object, Object> cache = originatingNode.cache(DEFAULT_CACHE_NAME);

        System.out.println("[txs]blocking message");

        blockMessage(GridDhtTxFinishResponse.class, false, backupNode1.id());
        blockMessage(GridDhtTxFinishResponse.class, true, backupNode2.id());

        Thread.sleep(1_000);

        try {
            System.out.println("[txs]before commiting");

            Integer finalKey = key;
            Ignite finalClient = originatingNode;
            // commit thread will be blocked until unblockMessage() is called
            IgniteInternalFuture<?> commitFut = multithreadedAsync(
                new Runnable() {
                    @Override public void run() {
                        Transaction tx0 = finalClient.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);

                        cache.put(finalKey, 1);

                        tx0.commit();

                        System.out.println(tx0.toString());
                    }
                }, 1);

            System.out.println("[txs]Transaction started. waiting 1 sec");

            Thread.sleep(1_000);

            assert !commitFut.isDone();
            System.out.println("[txs]Stopping node " + NODE_ROLE.ONLY_BACKUP);

            stopNode(NODE_ROLE.ONLY_BACKUP);

            unblockMessages(backupNode2.id());

            System.out.println("[txs]waiting after killing node");

            Thread.sleep(1_000);

            assertNull(commitFut.error().toString(), commitFut.error());

            System.out.println("[txs]commit must be finished");

            commitFut.get();
        }
        catch (Throwable ignored) {
            //No-op.
            System.out.println("[txs]error occured " + ignored);
        }
        finally {
            assertNotNull(originatingNode.cache(DEFAULT_CACHE_NAME).get(key));

            System.out.println("[txs]Test passed!");
        }
    }

    private Integer primaryKey(ClusterNode primaryNode, ClusterNode backupNode1, ClusterNode backupNode2) {
        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (clientNode.affinity(DEFAULT_CACHE_NAME).isPrimary(primaryNode, i))
                if (clientNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode1, i))
                    if (clientNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode2, i)) {
                        key = i;

                        break;
                    }
        }

        return key;
    }

    /**
     *
     */
    public void testPessimisticDeadlockDetectionTest() throws Exception {
        IgniteCache<Integer, Integer> clientCache = clientNode.cache(DEFAULT_CACHE_NAME);

        Transaction clientTx = clientNode.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, 3_000, 2);

        System.out.println("[txs]Before putting first " + 1);

        clientCache.put(1, 1);

        final AtomicInteger keyCnt = new AtomicInteger(1);

        System.out.println("[txs]Before running outer txs ");

        try {
            multithreaded(new Runnable() {
                @Override public void run() {

                    for (ClusterNode node : clientNode.cluster().forServers().nodes()) {
                        IgniteTxCreationRunnable txCreateRunnable = new IgniteTxCreationRunnable();

                        txCreateRunnable.firstKey = keyCnt.get() + 1;
                        txCreateRunnable.secondKey = keyCnt.getAndIncrement();
                        txCreateRunnable.nodeId = node.id();

                        System.out.println("[txs]Before computing " + keyCnt.get());

                        clientNode.compute(clientNode.cluster().forNodeId(node.id())).broadcast(txCreateRunnable);
                    }

                    System.out.println("[txs]Before checking cache values and locks");

                    for (int i = 1; i <= keyCnt.get(); i++) {
                        assertNull(clientCache.get(i));

                        Lock lock = clientCache.lock(i);

                        assertFalse(lock.tryLock());
                    }

                    System.out.println("[txs]After checking cache values and locks");
                }
            }, 1);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("[txs]After running outer txs ");

        assertEquals(clientNode.cluster().forServers().nodes().size() + 1, keyCnt.get());

        System.out.println("[txs]Before putting second" + keyCnt.get());

        try {
            clientCache.put(keyCnt.get(), keyCnt.get());

            System.out.println("[txs]Before committing " + keyCnt.get());

            clientTx.commit();
        }
        catch (Throwable e) {
            if (hasCause(e, TransactionTimeoutException.class) &&
                hasCause(e, TransactionDeadlockException.class)
                )
                e.printStackTrace();
            else
                assert false;
        }
        finally {
            clientTx.close();
        }

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return TransactionState.ROLLED_BACK.equals(clientTx.state());
            }
        }, 3_000);

        System.out.println("[txs]Before checking cache keys");

        Thread.sleep(1_000);

        multithreaded(new Runnable() {
            @Override public void run() {
                try {
                    GridTestUtils.waitForCondition(new GridAbsPredicate() {
                        @Override public boolean apply() {
                            try {
                                for (int i = 1; i < keyCnt.get(); i++) {
                                    assert clientCache != null;

                                    if (clientCache.get(i) == i + 1) {
                                        System.out.println("[txs]key verified " + i + " -> " + (i + 1));
                                    }
                                    else
                                        return false;

                                }

                                return clientCache.get(keyCnt.get()) == keyCnt.get();
                            }
                            catch (Throwable e) {
                                System.out.println("[txs]Smth wrong happened when calling get in client cache");

                                e.printStackTrace();

                                throw e;
                            }
                        }
                    }, 3_000);
                }
                catch (IgniteInterruptedCheckedException e) {
                    e.printStackTrace();
                }
            }
        }, 1);
    }

    public void testOptimisticSerializableDeadlockDetectionTest() throws IgniteInterruptedCheckedException {
        IgniteCache<Integer, Integer> clientCache = clientNode.cache(DEFAULT_CACHE_NAME);

        Lock lock = clientCache.lock(1);

        lock.lock();

        IgniteMultipleOptimisticTxCreationRunnable creationRunnable = new IgniteMultipleOptimisticTxCreationRunnable();

        creationRunnable.firstKey = 1;
        creationRunnable.secondKey = 2;

        Collection<ClusterNode> clusterNodes = clientNode.cluster().forServers().nodes();

        ArrayList<IgniteFuture> jobs = new ArrayList<>();

        for (ClusterNode node : clusterNodes)
            jobs.add(clientNode.compute(clientNode.cluster().forNode(node)).broadcastAsync(creationRunnable));

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (IgniteFuture job : jobs) {
                    if (!job.isDone())
                        return false;
                }

                return false;
            }
        }, 3_000);

        lock.unlock();

        for (IgniteFuture job : jobs) {
            try {
                job.get();

                fail();
            } catch (Throwable e){
                assertTrue(e instanceof TransactionOptimisticException);
            }
        }

        assertNull(clientCache.get(1));
        assertNull(clientCache.get(2));
    }

    private void unblockMessages(UUID... uuid) {
        releasingRunnable.nodeWhereBlock = uuid;

        ClusterGroup grp = clientNode.cluster().forNodeIds(Arrays.asList(uuid));

        clientNode.compute(grp).broadcast(releasingRunnable);
    }

    private void blockMessage(Class msgClass, boolean waitUnblock, UUID... nodesToBlock) {

        blockingRunnable.msgToBlock = msgClass;
        blockingRunnable.nodeWhereBlock = nodesToBlock;
        blockingRunnable.waitUnblock = waitUnblock;

        ClusterGroup grp = originatingNode.cluster().forRemotes();

        originatingNode.compute(grp).broadcast(blockingRunnable);
    }

    /**
     * @param ignite Node.
     * @return Communication SPI.
     */
    protected BlockTcpCommunicationSpi commSpi(Ignite ignite) {
        return ((BlockTcpCommunicationSpi)ignite.configuration().getCommunicationSpi());
    }
}