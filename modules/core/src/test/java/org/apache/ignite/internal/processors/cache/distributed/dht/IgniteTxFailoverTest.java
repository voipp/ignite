package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 *
 */
public class IgniteTxFailoverTest extends GridCommonAbstractTest {
    /** Client. */
    private IgniteEx client;

    /** Primary node. */
    private Ignite primaryNode;

    /** Backup node 1. */
    private Ignite backupNode1;

    /** Backup node 2. */
    private Ignite backupNode2;

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
        return 3;
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

        startGrids(gridCount());

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
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

        switch (scenarioId) {
            case 1:/* primary hasn't received prepare when failure occured. */
                commSpi1 = commSpi(client);

                commSpi1.blockMessage(GridNearTxPrepareRequest.class, failingNode != NODE_ROLE.ORIGINATING);

                break;
            case 2:/* backups haven't received prepare when failure occured. */
                commSpi1 = commSpi(primaryNode);

                commSpi1.blockMessage(GridDhtTxPrepareRequest.class, failingNode != NODE_ROLE.PRIMARY);

                break;
            case 3:/* primary hasn't received prepare response when failure occured. */
                commSpi1 = commSpi(backupNode1);

                commSpi1.blockMessage(GridDhtTxPrepareResponse.class, failingNode != NODE_ROLE.BOTH_BACKUPS &&
                    failingNode != NODE_ROLE.ONLY_BACKUP);
                commSpi2 = commSpi(backupNode2);

                commSpi2.blockMessage(GridDhtTxPrepareResponse.class, failingNode != NODE_ROLE.BOTH_BACKUPS);

                break;
            case 4:/* primary has received only one prepare response when failure occured.*/
                commSpi1 = commSpi(primaryNode);

                commSpi1.blockMessage(GridNearTxPrepareResponse.class, failingNode != NODE_ROLE.PRIMARY);
                commSpi2 = commSpi(backupNode2);

                commSpi2.blockMessage(GridDhtTxPrepareResponse.class, failingNode != NODE_ROLE.BOTH_BACKUPS);

                break;
            case 5:/* client hasn't received prepare finish when failure occured.*/
                commSpi1 = commSpi(primaryNode);

                commSpi1.blockMessage(GridNearTxPrepareResponse.class, failingNode != NODE_ROLE.PRIMARY);

                break;

            case 6:/* primary hasn't received finish request when failure occured.*/
                commSpi1 = commSpi(client);

                commSpi1.blockMessage(GridNearTxFinishRequest.class, failingNode != NODE_ROLE.ORIGINATING);

                break;
            case 7:/* backups haven't received finish when failure occured.*/
                commSpi1 = commSpi(primaryNode);

                commSpi1.blockMessage(GridDhtTxFinishRequest.class, failingNode != NODE_ROLE.PRIMARY);

                break;
            case 8:/* primary hasn't received finish responses when failure. */
                commSpi1 = commSpi(backupNode1);

                commSpi1.blockMessage(GridDhtTxFinishResponse.class, failingNode != NODE_ROLE.BOTH_BACKUPS &&
                    failingNode != NODE_ROLE.ONLY_BACKUP);
                commSpi2 = commSpi(backupNode2);

                commSpi2.blockMessage(GridDhtTxFinishResponse.class, failingNode != NODE_ROLE.BOTH_BACKUPS);

                break;
            case 9:/* primary has received only one finish response when failure occured.*/
                commSpi1 = commSpi(primaryNode);

                commSpi1.blockMessage(GridNearTxFinishResponse.class, failingNode != NODE_ROLE.PRIMARY);
                commSpi2 = commSpi(backupNode2);

                commSpi2.blockMessage(GridDhtTxFinishResponse.class, failingNode != NODE_ROLE.BOTH_BACKUPS);

                break;
            case 10:/* client hasn't received finish response when failure occured.*/
                commSpi1 = commSpi(primaryNode);

                commSpi1.blockMessage(GridNearTxFinishResponse.class, failingNode != NODE_ROLE.PRIMARY);

                break;
        }

        return new IgniteBiTuple<>(commSpi1, commSpi2);
    }

    /**
     *     possible scenarios :
     1) failure when prepare is not received on primary
     2) failure when primary received prepare but not sent to backups.
     3) failure when primary sent prepare to backups.
     4) failure when primary received prepare response only from one backup out of 2.
     5) failure when primary received prepare responses from all backups.

     6) failure when finish is not received on primary
     7) failure when primary received finish but not sent to backups.
     8) failure when primary sent finish to backups.
     9) failure when primary received finish response only from one backup out of 2.
     10) failure when primary received finish responses from all backups.

        possible crashing nodes:
     1) primary node
     2) both backups
     3) only one backup
     4) originating node(new client will be started)
     *
     * @param failingNode Failing node.
     * @param scenarioId Scenario id.
     */
    private void testNodeFailureWhileTransaction(NODE_ROLE failingNode, int scenarioId) throws Exception {
        primaryNode = ignite(1);
        backupNode1 = ignite(0);
        backupNode2 = ignite(2);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(3));
        cfg.setClientMode(true);

        client = startGrid(cfg);

        awaitPartitionMapExchange();

        //key that would be primary on grid1, and backup on grid2, grid3
        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (client.affinity(DEFAULT_CACHE_NAME).isPrimary(primaryNode.cluster().localNode(), i))
                if (client.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode1.cluster().localNode(), i))
                    if (client.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode2.cluster().localNode(), i)) {
                        key = i;

                        break;
                    }
        }

        assert key != null;

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        IgniteBiTuple<BlockTcpCommunicationSpi, BlockTcpCommunicationSpi> commSpi = blockMessages(scenarioId, failingNode);

        try {
            Integer finalKey = key;
            IgniteEx finalClient = client;
            IgniteInternalFuture<?> commitFut = multithreadedAsync(
                new Runnable() {
                    @Override public void run() {
                        Transaction tx0 = finalClient.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);

                        cache.put(finalKey, 1);

                        tx0.commit();
                    }
                }, 1);

            Thread.sleep(1_000);

            assert !commitFut.isDone();

            stopNode(failingNode);

            if (commSpi.get1() != null)
                commSpi.get1().unblockMessages();

            if (commSpi.get2() != null)
                commSpi.get2().unblockMessages();

            Thread.sleep(1_000);

            if (failingNode == NODE_ROLE.ONLY_BACKUP || failingNode == NODE_ROLE.BOTH_BACKUPS
                || (failingNode == NODE_ROLE.PRIMARY && scenarioId == 7)
                || (failingNode == NODE_ROLE.PRIMARY && scenarioId == 8))
                assertNull(commitFut.error());
            else
                assertNotNull(commitFut.error());

            commitFut.get();
        }
        catch (Exception ignored) {
            //No-op.
        }
        finally {
            if (failingNode == NODE_ROLE.ORIGINATING) {
                client = startGrid(getConfiguration(getTestIgniteInstanceName(4)).setClientMode(true));

                awaitPartitionMapExchange();

                assertNotNull(client.cache(DEFAULT_CACHE_NAME).get(key));
            }
            else if (failingNode == NODE_ROLE.ONLY_BACKUP || failingNode == NODE_ROLE.BOTH_BACKUPS)
                assertNotNull(client.cache(DEFAULT_CACHE_NAME).get(key));
            else if (failingNode == NODE_ROLE.PRIMARY) {

                if (scenarioId == 7 || scenarioId == 8)
                    assertNotNull(client.cache(DEFAULT_CACHE_NAME).get(key));
                else
                    assertNull(client.cache(DEFAULT_CACHE_NAME).get(key));
            }
        }
    }

    /**
     * @param failingNode Failing node.
     */
    private void stopNode(NODE_ROLE failingNode) {
        switch (failingNode) {
            case PRIMARY:/* primary */
                assert G.stop(primaryNode.name(), true);

                break;
            case BOTH_BACKUPS:/* backups */
                assert G.stop(backupNode1.name(), true);
                assert G.stop(backupNode2.name(), true);

                break;
            case ONLY_BACKUP/* only one backup */ :
                assert G.stop(backupNode1.name(), true);

                break;
            case ORIGINATING/* originating node */ :
                assert G.stop(client.name(), true);

                break;
        }
    }

    /**
     *
     */
    public void testPrimaryReceivedPrepareAndFailed() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.PRIMARY, 2);
    }

    /**
     *
     */
    public void testPrimarySentPrepareAndFailed() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.PRIMARY, 3);
    }

    /**
     *
     */
    public void testPrimaryReceivedFinishAndFailed() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.PRIMARY, 7);
    }

    /**
     *
     */
    public void testPrimarySentFinishAndFailed() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.PRIMARY, 8);
    }

    /**
     *
     */
    public void testBackupsReceivedPrepareAndFailed() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.ONLY_BACKUP, 3);
    }

    /**
     *
     */
    public void testPrimaryReceivedPrepareFromOneBackupAndBackupFailed() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.ONLY_BACKUP, 4);
    }

    /**
     *
     */
    public void testPrimaryReceivedPrepareFromBackupsAndBackupFailed() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.ONLY_BACKUP, 5);
    }

    /**
     *
     */
    public void testBackupsReceivedFinishedAndFailed() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.ONLY_BACKUP, 8);
    }

    /**
     *
     */
    public void testPrimaryReceivedfinishFromBackupsAndBackupFailed() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.ONLY_BACKUP, 9);
    }

    /**
     *
     */
    public void testPrimaryReceivedFinishFromBackupsAndBackupFailed() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.ONLY_BACKUP, 10);
    }

    /**
     *
     */
    public void testPrimaryReceivedPrepareAndCoordinatorFailed() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.ORIGINATING, 2);
    }

    /**
     *
     */
    public void testPrimaryReceivedFinishAndCoordinatorFailed() throws Exception {
        testNodeFailureWhileTransaction(NODE_ROLE.ORIGINATING, 7);
    }

    /**
     * @param ignite Node.
     * @return Communication SPI.
     */
    protected BlockTcpCommunicationSpi commSpi(Ignite ignite) {
        return ((BlockTcpCommunicationSpi)ignite.configuration().getCommunicationSpi());
    }

    /**
     *
     */
    protected static class BlockTcpCommunicationSpi extends TcpCommunicationSpi {
        /** */
        volatile Class msgCls;

        /** Message latch. */
        private CountDownLatch msgLatch;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            Class msgCls0 = msgCls;

            if (msgCls0 != null && msg instanceof GridIoMessage
                && ((GridIoMessage)msg).message().getClass().equals(msgCls)) {

                try {
                    if (msgLatch != null)
                        msgLatch.await();
                    else
                        return;
                }
                catch (InterruptedException e) {
                    assert false;
                }
            }

            super.sendMessage(node, msg, ackC);
        }

        /**
         * @param clazz Class of messages which will be block.
         */
        public void blockMessage(Class clazz, boolean waitUnblock) {
            msgCls = clazz;

            if (waitUnblock)
                msgLatch = new CountDownLatch(1);
            else
                msgLatch = null;
        }

        /**
         * Unlock all message.
         */
        public void unblockMessages() {
            msgCls = null;

            if (msgLatch != null)
                msgLatch.countDown();
        }
    }
}