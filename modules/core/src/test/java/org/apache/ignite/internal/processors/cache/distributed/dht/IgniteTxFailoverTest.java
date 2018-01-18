package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.communication.tcp.BlockTcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.IgniteBlockingRunnable;
import org.apache.ignite.spi.communication.tcp.IgniteKamikazeRunnable;
import org.apache.ignite.spi.communication.tcp.IgniteReleasingRunnable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

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

    public void test1() throws Exception {
        Ignite grid = G.start("C:\\work\\ignite\\bin\\config.xml");

        awaitPartitionMapExchange();

        for (ClusterNode node : grid.cluster().forServers().nodes()) {
            System.out.println(" node id " + node.id());

        }

        IgniteKamikazeRunnable kamikazeRunnable = new IgniteKamikazeRunnable();

        kamikazeRunnable.nodesToKill = new UUID[]{grid.cluster().forServers().node().id()};

        grid.compute(grid.cluster().forServers()).broadcast(kamikazeRunnable);

//        IgniteBlockingRunnable blockingRunnable = new IgniteBlockingRunnable();
//
//        blockingRunnable.msgToBlock = GridNearTxFinishRequest.class;
//
//        grid.compute(grid.cluster().forServers()).broadcast(blockingRunnable);
//
//        grid.compute(grid.cluster().forServers()).broadcast(new IgniteReleasingRunnable());

    }

    /**
     * @param scenarioId Scenario id.
     * @param failingNode Failing node.
     */
    public IgniteBiTuple<BlockTcpCommunicationSpi, BlockTcpCommunicationSpi> blockMessages(
        int scenarioId,
        NODE_ROLE failingNode) {
        BlockTcpCommunicationSpi commSpi1 = null;
        BlockTcpCommunicationSpi commSpi2 = null;
        ClusterGroup grp = originatingNode.cluster().forRemotes();

        switch (scenarioId) {
            case 1:/* primary hasn't received prepare when failure occured. */
                commSpi1 = commSpi(originatingNode);

                commSpi1.blockMessage(GridNearTxPrepareRequest.class, failingNode != NODE_ROLE.ORIGINATING);

                break;
            case 2:/* backups haven't received prepare when failure occured. */
                //commSpi1 = commSpi(primaryNode);

                //commSpi1.blockMessage(GridDhtTxPrepareRequest.class, failingNode != NODE_ROLE.PRIMARY);

                blockingRunnable.msgToBlock = GridDhtTxPrepareRequest.class;
                blockingRunnable.nodeWhereBlock = new UUID[] {primaryNode.id()};
                blockingRunnable.waitUnblock = false;

                originatingNode.compute(grp).broadcast(blockingRunnable);

                System.out.println("[txs]Block job is executed on");
                for (ClusterNode node : grp.nodes())
                    System.out.print(" " + node.id());

                System.out.println("\n");

                break;
            case 3:/* primary hasn't received prepare response when failure occured. */
//                commSpi1 = commSpi(backupNode1);
//
//                commSpi1.blockMessage(GridDhtTxPrepareResponse.class, failingNode != NODE_ROLE.BOTH_BACKUPS &&
//                    failingNode != NODE_ROLE.ONLY_BACKUP);
//                commSpi2 = commSpi(backupNode2);
//
//                commSpi2.blockMessage(GridDhtTxPrepareResponse.class, failingNode != NODE_ROLE.BOTH_BACKUPS);

                blockingRunnable.msgToBlock = GridDhtTxPrepareResponse.class;
                blockingRunnable.nodeWhereBlock = new UUID[] {backupNode1.id(), backupNode2.id()};
                blockingRunnable.waitUnblock = failingNode != NODE_ROLE.BOTH_BACKUPS && failingNode != NODE_ROLE.ONLY_BACKUP;

                originatingNode.compute(grp).broadcast(blockingRunnable);

                break;
            case 4:/* primary has received only one prepare response when failure occured.*/
                //commSpi1 = commSpi(primaryNode);

                //commSpi1.blockMessage(GridNearTxPrepareResponse.class, failingNode != NODE_ROLE.PRIMARY);
                //commSpi2 = commSpi(backupNode2);

                //commSpi2.blockMessage(GridDhtTxPrepareResponse.class, failingNode != NODE_ROLE.BOTH_BACKUPS);

                blockingRunnable.msgToBlock = GridDhtTxPrepareResponse.class;
                blockingRunnable.nodeWhereBlock = new UUID[] {primaryNode.id(), backupNode2.id()};
                blockingRunnable.waitUnblock = failingNode != NODE_ROLE.BOTH_BACKUPS && failingNode != NODE_ROLE.ONLY_BACKUP;

                originatingNode.compute(grp).broadcast(blockingRunnable);

                break;
            case 5:/* originatingNode hasn't received prepare finish when failure occured.*/
                //commSpi1 = commSpi(primaryNode);

                commSpi1.blockMessage(GridNearTxPrepareResponse.class, failingNode != NODE_ROLE.PRIMARY);

                break;

            case 6:/* primary hasn't received finish request when failure occured.*/
                commSpi1 = commSpi(originatingNode);

                commSpi1.blockMessage(GridNearTxFinishRequest.class, failingNode != NODE_ROLE.ORIGINATING);

                break;
            case 7:/* backups haven't received finish when failure occured.*/
                // commSpi1 = commSpi(primaryNode);

                commSpi1.blockMessage(GridDhtTxFinishRequest.class, failingNode != NODE_ROLE.PRIMARY);

                break;
            case 8:/* primary hasn't received finish responses when failure. */
                //commSpi1 = commSpi(backupNode1);

                commSpi1.blockMessage(GridDhtTxFinishResponse.class, failingNode != NODE_ROLE.BOTH_BACKUPS &&
                    failingNode != NODE_ROLE.ONLY_BACKUP);
                //commSpi2 = commSpi(backupNode2);

                commSpi2.blockMessage(GridDhtTxFinishResponse.class, failingNode != NODE_ROLE.BOTH_BACKUPS);

                break;
            case 9:/* primary has received only one finish response when failure occured.*/
                //commSpi1 = commSpi(primaryNode);

                commSpi1.blockMessage(GridNearTxFinishResponse.class, failingNode != NODE_ROLE.PRIMARY);
                //commSpi2 = commSpi(backupNode2);

                commSpi2.blockMessage(GridDhtTxFinishResponse.class, failingNode != NODE_ROLE.BOTH_BACKUPS);

                break;
            case 10:/* originatingNode hasn't received finish response when failure occured.*/
                //commSpi1 = commSpi(primaryNode);

                commSpi1.blockMessage(GridNearTxFinishResponse.class, failingNode != NODE_ROLE.PRIMARY);

                break;
        }

        return new IgniteBiTuple<>(commSpi1, commSpi2);
    }

    /**
     * possible scenarios :
     * 1) failure when prepare is not received on primary
     * 2) failure when primary received prepare but not sent to backups.
     * 3) failure when primary sent prepare to backups.
     * 4) failure when primary received prepare response only from one backup out of 2.
     * 5) failure when primary received prepare responses from all backups.
     *
     * 6) failure when finish is not received on primary
     * 7) failure when primary received finish but not sent to backups.
     * 8) failure when primary sent finish to backups.
     * 9) failure when primary received finish response only from one backup out of 2.
     * 10) failure when primary received finish responses from all backups.
     *
     * possible crashing nodes:
     * 1) primary node
     * 2) both backups
     * 3) only one backup
     * 4) originating node(new originatingNode will be started)
     *
     * @param failingNode Failing node.
     * @param scenarioId Scenario id.
     */
    private void testNodeFailureWhileTransaction(NODE_ROLE failingNode, int scenarioId) throws Exception {
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

        IgniteBiTuple<BlockTcpCommunicationSpi, BlockTcpCommunicationSpi> commSpi = blockMessages(scenarioId, failingNode);

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
            System.out.println("[txs]Stopping node " + failingNode);

            stopNode(failingNode);

//            if (commSpi.get1() != null)
//                commSpi.get1().unblockMessages();
//
//            if (commSpi.get2() != null)
//                commSpi.get2().unblockMessages();

            System.out.println("[txs]waiting after killing node");

            Thread.sleep(1_000);

            if (failingNode == NODE_ROLE.ONLY_BACKUP || failingNode == NODE_ROLE.BOTH_BACKUPS
                || (failingNode == NODE_ROLE.PRIMARY && scenarioId == 7)
                || (failingNode == NODE_ROLE.PRIMARY && scenarioId == 8))
                assertNull(commitFut.error().toString(), commitFut.error());
            else
                assertNotNull(commitFut.toString(), commitFut.error());

            System.out.println("[txs]commit must be finished");

            commitFut.get();
        }
        catch (Throwable ignored) {
            //No-op.
            System.out.println("[txs]error occured " + ignored);
        }
        finally {
            if (failingNode == NODE_ROLE.ORIGINATING) {
                originatingNode = startGrid(getConfiguration(getTestIgniteInstanceName(4)).setClientMode(true));

                awaitPartitionMapExchange();

                assertNotNull(originatingNode.cache(DEFAULT_CACHE_NAME).get(key));
            }
            else if (failingNode == NODE_ROLE.ONLY_BACKUP || failingNode == NODE_ROLE.BOTH_BACKUPS)
                assertNotNull(originatingNode.cache(DEFAULT_CACHE_NAME).get(key));
            else if (failingNode == NODE_ROLE.PRIMARY) {

                if (scenarioId == 7 || scenarioId == 8)
                    assertNotNull(originatingNode.cache(DEFAULT_CACHE_NAME).get(key));
                else
                    assertNull(originatingNode.cache(DEFAULT_CACHE_NAME).get(key));
            }

            System.out.println("[txs]Test passed!");
        }
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

    /**
     *
     */
    private void testPrimaryReceivedPrepareAndFailed1() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.PRIMARY, 2);
    }

    /**
     *
     */
//    public void testPrimarySentPrepareAndFailed() throws Exception {
//        testNodeFailureWhileTransaction(NODE_ROLE.PRIMARY, 3);
//    }

    /**
     *
     */
    private void testPrimaryReceivedFinishAndFailed1() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.PRIMARY, 7);
    }

    /**
     *
     */
//    public void testPrimarySentFinishAndFailed() throws Exception {
//        testNodeFailureWhileTransaction(NODE_ROLE.PRIMARY, 8);
//    }

    /**
     *
     */
    private void testBackupsReceivedPrepareAndFailed1() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.ONLY_BACKUP, 3);
    }

    /**
     *
     */
//    public void testPrimaryReceivedPrepareFromOneBackupAndBackupFailed() throws Exception {
//        testNodeFailureWhileTransaction(NODE_ROLE.ONLY_BACKUP, 4);
//    }

    /**
     *
     */
//    public void testPrimaryReceivedPrepareFromBackupsAndBackupFailed() throws Exception {
//        testNodeFailureWhileTransaction(NODE_ROLE.ONLY_BACKUP, 5);
//    }

    /**
     *
     */
//    public void testBackupsReceivedFinishedAndFailed() throws Exception {
//        testNodeFailureWhileTransaction(NODE_ROLE.ONLY_BACKUP, 8);
//    }

    /**
     *
     */
//    public void testPrimaryReceivedfinishFromBackupsAndBackupFailed() throws Exception {
//        testNodeFailureWhileTransaction(NODE_ROLE.ONLY_BACKUP, 9);
//    }

    /**
     *
     */
//    public void testPrimaryReceivedFinishFromBackupsAndBackupFailed() throws Exception {
//        testNodeFailureWhileTransaction(NODE_ROLE.ONLY_BACKUP, 10);
//    }

    /**
     *
     */
    private void testPrimaryReceivedPrepareAndCoordinatorFailed1() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.ORIGINATING, 2);
    }

    /**
     *
     */
    private void testPrimaryReceivedFinishAndCoordinatorFailed1() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.ORIGINATING, 7);
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

    public void testPrimaryReceivedPrepareAndCoordinatorFailed() throws Exception {
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

        blockMessage(GridDhtTxPrepareRequest.class, true, primaryNode.id());

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

    public void testPrimaryReceivedFinishAndCoordinatorFailed() throws Exception {
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