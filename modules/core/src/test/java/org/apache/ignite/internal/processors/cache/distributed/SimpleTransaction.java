package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import javax.cache.Cache;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractLocalStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheIoManager;
import org.apache.ignite.internal.processors.cache.GridCacheMvcc;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.store.CacheLocalStore;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxHandler;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

/**
 * Created by SBT-Kuznetsov-AL on 29.05.2017.
 */
public class SimpleTransaction extends GridCacheAbstractSelfTest {

    public static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(false) {{
        setAddresses(Collections.singleton("127.0.0.1:47500..47509"));
    }};

    /** */
    public static final TestLocalStore<Integer, Integer> LOCAL_STORE_0 = new TestLocalStore<>();

    /** */
    public static final TestLocalStore<Integer, Integer> LOCAL_STORE_1 = new TestLocalStore<>();

    /** */
    public static final TestLocalStore<Integer, Integer> LOCAL_STORE_2 = new TestLocalStore<>();

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

    /** */
    private static class DummyAffinity extends RendezvousAffinityFunction {

        /**
         * Default constructor.
         */
        public DummyAffinity() {
            super(false, 2);
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<ClusterNode> nodes = affCtx.currentTopologySnapshot();

            List<List<ClusterNode>> assign = new ArrayList<>(2);

            if (nodes.size() == 1){
                assign.add(Collections.singletonList(nodes.get(0)));
                assign.add(Collections.singletonList(nodes.get(0)));
            } else if(nodes.size() == 2){
                assign.add(Collections.singletonList(nodes.get(0)));
                assign.add(Collections.singletonList(nodes.get(1)));
            }

            return assign;
        }

        @Override public int partition(Object key) {
            return (int)key - 1;
        }
    }

    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration config = super.getConfiguration(gridName);

        CacheConfiguration configuration = defaultCacheConfiguration();
        configuration.setBackups(1);
        configuration.setAffinity(new DummyAffinity());
        configuration.setRebalanceMode(CacheRebalanceMode.SYNC);
        configuration.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        //configuration.setCacheStoreFactory(new StoreFactory());
        //configuration.setWriteThrough(true);
        //configuration.setReadThrough(true);
        //configuration.setWriteBehindEnabled(true);

        config.setCacheConfiguration(configuration);
        config.setDiscoverySpi(new TcpDiscoverySpi());

        ((TcpDiscoverySpi)config.getDiscoverySpi()).setIpFinder(IP_FINDER);

        configuration.setNearConfiguration(new NearCacheConfiguration());

        return config;
    }

    @Override protected boolean isMultiJvm() {
        return false;
    }

    //    @Override protected NearCacheConfiguration nearConfiguration() {
//        return null;
//    }

    public void testRebalancing() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);
        cache.put(2, 2);

        awaitPartitionMapExchange();

        for (int i : grid(0).affinity(DEFAULT_CACHE_NAME).primaryPartitions(grid(0).localNode()))
            System.out.println("[xchg]partition on 0 grid " + i);

        IgniteEx grid2 = startGrid(1);

        awaitPartitionMapExchange();

        for (int i : grid(0).affinity(DEFAULT_CACHE_NAME).primaryPartitions(grid(0).localNode()))
            System.out.println("[xchg]partition on 0 grid " + i);

        for (int i : grid(0).affinity(DEFAULT_CACHE_NAME).primaryPartitions(grid(1).localNode()))
            System.out.println("[xchg]partition on 1 grid " + i);

        assertEquals(1, grid(0).cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY));
        assertEquals(1, grid(1).cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY));

        System.out.println("closing caches");
    }

    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE - 1;
    }

    // If you want to test node failures don't forget about ShotdownHook on primary node, and kill it when near tx prepare received(by executing System.exit())!
    public void testPrimaryNodeFailure() throws Exception {
        //IgniteTxHandler.crashOnPrimary = true;

        Ignite otherNode = ignite(0);

        Ignite primaryNode = startGrid(1);//startGrid(getTestIgniteInstanceName(1), getConfiguration(getTestIgniteInstanceName(1)));
        Ignite backupNode1 = startGrid(2);//startGrid(getTestIgniteInstanceName(2), getConfiguration(getTestIgniteInstanceName(2)));
        Ignite backupNode2 = startGrid(3);//startGrid(getTestIgniteInstanceName(3), getConfiguration(getTestIgniteInstanceName(3)));

        awaitPartitionMapExchange();

        Integer key = null;

        for (int i = 0; i < 1024; i++) {
            if(otherNode.affinity(DEFAULT_CACHE_NAME).isPrimary(primaryNode.cluster().localNode(), i))
                if (otherNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode1.cluster().localNode(), i))
                    if (otherNode.affinity(DEFAULT_CACHE_NAME).isBackup(backupNode2.cluster().localNode(), i)) {
                        key = i;

                        break;
                    }
        }

        assert  key != null;

        IgniteCache<Object, Object> cache = otherNode.cache(DEFAULT_CACHE_NAME);

        Transaction tx = otherNode.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);

        cache.put(key, 1);

        try {
            tx.commit();
        }catch (Exception e){
            e.printStackTrace();
        }

        System.out.println("[txs]cache.get(key) " + cache.get(key) + ". tx.state() " + tx.state());
    }

    public void testMain() throws Exception {
        //GridCacheMvcc.stopLogging = true;
        Ignite ignite0 = ignite(0);
        Ignite ignite1 = ignite(1);
        Ignite ignite2 = ignite(2);

        ignite0.cache(DEFAULT_CACHE_NAME).getConfiguration(CacheConfiguration.class).setStatisticsEnabled(true);
        ignite1.cache(DEFAULT_CACHE_NAME).getConfiguration(CacheConfiguration.class).setStatisticsEnabled(true);
        ignite2.cache(DEFAULT_CACHE_NAME).getConfiguration(CacheConfiguration.class).setStatisticsEnabled(true);

//        if(cacheMode() == CacheMode.LOCAL){
//            GridCacheMvccCandidate.startDumping = true;
//
//            IgniteTransactions txs = ignite0.transactions();
//
//            Transaction tx = txs.txStart();
//
//            ignite0.cache(DEFAULT_CACHE_NAME).put(1, 1);
//
//            tx.commit();
//
//            Thread.sleep(2_000);
//
//            GridCacheMvccCandidate.startDumping = false;
//
//            return;
//        }

        IgniteCache<String, Integer> jcache = jcache();

        Assert.assertTrue(ignite0 != null && ignite1 != null && jcache != null);
        Assert.assertTrue(jcache.getName().equals(DEFAULT_CACHE_NAME));

        if (cacheMode() != CacheMode.REPLICATED && cacheMode() != CacheMode.LOCAL)
        Assert.assertEquals(1, jcache.getConfiguration(CacheConfiguration.class).getBackups());

        Assert.assertTrue(String.valueOf(ignite(0).configuration().getTransactionConfiguration().getDefaultTxTimeout()),
            ignite(0).configuration().getTransactionConfiguration().getDefaultTxTimeout() == 0);

        String key1 = "key1";
        ClusterNode primaryNode = ignite0.affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key1).toArray(new ClusterNode[3])[0];
        ClusterNode backupNode = ignite0.affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key1).toArray(new ClusterNode[3])[1];

        System.out.println("ignite(0)=" + ignite0.cluster().localNode().id());
        System.out.println("ignite(1)=" + ignite1.cluster().localNode().id());
        System.out.println("ignite(2)=" + ignite2.cluster().localNode().id());
        System.out.println("primary= " + primaryNode.id());
        if (backupNode != null)
        System.out.println("backup= " + backupNode.id());

        Ignite otherIgnite = null;
        if(!primaryNode.id().equals(ignite0.cluster().localNode().id()) && !backupNode.id().equals(ignite0.cluster().localNode().id())){
            otherIgnite = ignite0;
        } else if(!primaryNode.id().equals(ignite1.cluster().localNode().id()) && !backupNode.id().equals(ignite1.cluster().localNode().id())){
            otherIgnite = ignite1;
        } else if(!primaryNode.id().equals(ignite2.cluster().localNode().id()) && !backupNode.id().equals(ignite2.cluster().localNode().id())){
            otherIgnite = ignite2;
        }else fail("1");
        IgniteTransactions otherTransactions = otherIgnite.transactions();
        IgniteCache<Object, Object> otherCache = otherIgnite.cache(DEFAULT_CACHE_NAME);

        Ignite primaryIgnite = null;
        if(ignite0.cluster().localNode().id().equals(primaryNode.id())){
            primaryIgnite = ignite0;
        } else if(ignite1.cluster().localNode().id().equals(primaryNode.id())){
            primaryIgnite = ignite1;
        } else if(ignite2.cluster().localNode().id().equals(primaryNode.id())){
            primaryIgnite = ignite2;
        }

        IgniteTransactions primaryTransactions = primaryIgnite.transactions();

        IgniteCache<Object, Object> primaryCache = primaryIgnite.cache(DEFAULT_CACHE_NAME);

        String primaryKey = new String();

        Affinity<String> aff = affinity(primaryIgnite.cache(DEFAULT_CACHE_NAME));
        if(aff.isPrimary(primaryNode, "1"))
            primaryKey = "1";
        else if(aff.isPrimary(primaryNode, "2"))
            primaryKey = "2";
        else if(aff.isPrimary(primaryNode, "3"))
            primaryKey = "3";
        else if(aff.isPrimary(primaryNode, "4"))
            primaryKey = "4";
        else if(aff.isPrimary(primaryNode, "5"))
            primaryKey = "5";
        else if(aff.isPrimary(primaryNode, "6"))
            primaryKey = "6";
        else assert false;

        assert ignite0.affinity(DEFAULT_CACHE_NAME).mapKeyToNode(primaryKey).equals(primaryNode);

        Ignite backupIgnite = null;
        if(ignite0.cluster().localNode().id().equals(backupNode.id())){
            backupIgnite = ignite0;
        } else if(ignite1.cluster().localNode().id().equals(backupNode.id())){
            backupIgnite = ignite1;
        } else if(ignite2.cluster().localNode().id().equals(backupNode.id())){
            backupIgnite = ignite2;
        }

        Transaction tx = otherTransactions.txStart();

        otherCache.put(primaryKey, 1);

        tx.commit();

        Thread.sleep(3_000);
        //assertEquals(1, otherCache.get(key1));
//
//        System.out.println("ignite(0)=" + ignite0.cluster().localNode().id());
//        System.out.println("ignite(1)=" + ignite1.cluster().localNode().id());
//        System.out.println("ignite(2)=" + ignite2.cluster().localNode().id());
//        System.out.println("primary= " + primaryNode.id());
//        System.out.println("backup= " + backupNode.id());
//        System.out.println("Other node user open cache key peek: " + otherIgnite.cache(DEFAULT_CACHE_NAME).localPeek("key1"));
//        System.out.println("Other node user open cache class: " + otherIgnite.cache(DEFAULT_CACHE_NAME).getClass().getSimpleName());
//
//        GridNearCacheAdapter<Object, Object> near = ((IgniteKernal)otherIgnite).internalCache(DEFAULT_CACHE_NAME).context().near();
//        System.out.println("Other node near inner cache class: " + near.getClass());
//        System.out.println("Other node near inner cache key peek: " + near.peekEx("key1"));
//
//        System.out.println("Other node dht inner cache class: " + near.dht().getClass());
//        System.out.println("Other node dht inner cache key peek: " + near.dht().peekEx("key1"));
//
//        //------------------------------------------------------------------------------------------------------------------------------
//
//        System.out.println("Primary user open cache key peek: " + primaryIgnite.cache(DEFAULT_CACHE_NAME).localPeek("key1"));
//        System.out.println("Primary node user open cache class: " + primaryIgnite.cache(DEFAULT_CACHE_NAME).getClass().getSimpleName());
//
//        GridNearCacheAdapter<Object, Object> PrimaryNear = ((IgniteKernal)primaryIgnite).internalCache(DEFAULT_CACHE_NAME).context().near();
//        System.out.println("Primary node near inner cache class: " + PrimaryNear.getClass());
//        System.out.println("Primary node near inner cache key peek: " + PrimaryNear.peekEx("key1"));
//
//        System.out.println("Primary node dht inner cache class: " + PrimaryNear.dht().getClass());
//        System.out.println("Primary node dht inner cache key peek: " + PrimaryNear.dht().peekEx("key1"));
//
//        //------------------------------------------------------------------------------------------------------------------------------
//
//        System.out.println("Backup user open cache key peek: " + backupIgnite.cache(DEFAULT_CACHE_NAME).localPeek("key1"));
//        System.out.println("Backup node user open cache class: " + backupIgnite.cache(DEFAULT_CACHE_NAME).getClass().getSimpleName());
//
//        GridNearCacheAdapter<Object, Object> BackupNear = ((IgniteKernal)backupIgnite).internalCache(DEFAULT_CACHE_NAME).context().near();
//        System.out.println("Backup node near inner cache class: " + BackupNear.getClass());
//        System.out.println("Backup node near inner cache key peek: " + BackupNear.peekEx("key1"));
//
//        System.out.println("Backup node dht inner cache class: " + BackupNear.dht().getClass());
//        System.out.println("Backup node dht inner cache key peek: " + BackupNear.dht().peekEx("key1"));

        //------------------------------------------------------------------------------------------------------------------------------
        //cache.get("key1");
    }

    public void testDeadlock() throws Exception {
        //GridCacheMvcc.stopLogging = true;


        Ignite ignite0 = ignite(0);
        Ignite ignite1 = ignite(1);
        Ignite ignite2 = ignite(2);

        IgniteCache<String, Integer> cache = jcache();

        Assert.assertTrue(ignite0 != null && ignite1 != null && cache != null);
        Assert.assertTrue(cache.getName().equals(DEFAULT_CACHE_NAME));
        Assert.assertTrue(cache.getConfiguration(CacheConfiguration.class).getBackups() == 1);
        Assert.assertTrue(cache.getConfiguration(CacheConfiguration.class).getAtomicityMode().equals(CacheAtomicityMode.TRANSACTIONAL));
        Assert.assertTrue(cache.getConfiguration(CacheConfiguration.class).getCacheMode().equals(CacheMode.PARTITIONED));

        ClusterNode primaryNode = ignite0.affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups("key1").toArray(new ClusterNode[3])[0];
        ClusterNode backupNode = ignite0.affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups("key1").toArray(new ClusterNode[3])[1];

        System.out.println("ignite(0)=" + ignite0.cluster().localNode().id());
        System.out.println("ignite(1)=" + ignite1.cluster().localNode().id());
        System.out.println("ignite(2)=" + ignite2.cluster().localNode().id());
        System.out.println("primary= " + primaryNode.id());
        System.out.println("backup= " + backupNode.id());

        Ignite otherIgnite = null;
        if(!primaryNode.id().equals(ignite0.cluster().localNode().id()) && !backupNode.id().equals(ignite0.cluster().localNode().id())){
            otherIgnite = ignite0;
        } else if(!primaryNode.id().equals(ignite1.cluster().localNode().id()) && !backupNode.id().equals(ignite1.cluster().localNode().id())){
            otherIgnite = ignite1;
        } else if(!primaryNode.id().equals(ignite2.cluster().localNode().id()) && !backupNode.id().equals(ignite2.cluster().localNode().id())){
            otherIgnite = ignite2;
        }else fail("1");

        Ignite primaryIgnite = null;
        if(ignite0.cluster().localNode().id().equals(primaryNode.id())){
            primaryIgnite = ignite0;
        } else if(ignite1.cluster().localNode().id().equals(primaryNode.id())){
            primaryIgnite = ignite1;
        } else if(ignite2.cluster().localNode().id().equals(primaryNode.id())){
            primaryIgnite = ignite2;
        }

        Ignite backupIgnite = null;
        if(ignite0.cluster().localNode().id().equals(backupNode.id())){
            backupIgnite = ignite0;
        } else if(ignite1.cluster().localNode().id().equals(backupNode.id())){
            backupIgnite = ignite1;
        } else if(ignite2.cluster().localNode().id().equals(backupNode.id())){
            backupIgnite = ignite2;
        }

        String primaryKey = new String();

        Affinity<String> aff = affinity(primaryIgnite.cache(DEFAULT_CACHE_NAME));
        if(aff.isPrimary(primaryNode, "1"))
            primaryKey = "1";
        else if(aff.isPrimary(primaryNode, "2"))
            primaryKey = "2";
        else if(aff.isPrimary(primaryNode, "3"))
            primaryKey = "3";
        else if(aff.isPrimary(primaryNode, "4"))
            primaryKey = "4";
        else if(aff.isPrimary(primaryNode, "5"))
            primaryKey = "5";
        else if(aff.isPrimary(primaryNode, "6"))
            primaryKey = "6";
        else assert false;

        String backupKey = new String();

        Affinity<String> backupAff = affinity(backupIgnite.cache(DEFAULT_CACHE_NAME));
        if(backupAff.isPrimary(backupNode, "1"))
            backupKey = "1";
        else if(backupAff.isPrimary(backupNode, "2"))
            backupKey = "2";
        else if(backupAff.isPrimary(backupNode, "3"))
            backupKey = "3";
        else if(backupAff.isPrimary(backupNode, "4"))
            backupKey = "4";
        else if(backupAff.isPrimary(backupNode, "5"))
            backupKey = "5";
        else if(backupAff.isPrimary(backupNode, "6"))
            backupKey = "6";
        else assert false;

        IgniteCache<Object, Object> backupCache = backupIgnite.cache(DEFAULT_CACHE_NAME);

        //IgniteTransactions otherTransactions = primaryIgnite.otherTransactions();
        IgniteTransactions otherTransactions = otherIgnite.transactions();
        IgniteTransactions primaryTransactions = primaryIgnite.transactions();

        IgniteCache<Object, Object> primaryCache = primaryIgnite.cache(DEFAULT_CACHE_NAME);

        CyclicBarrier barrier = new CyclicBarrier(2);

        String finalBackupKey = backupKey;
        String finalPrimaryKey = primaryKey;
        multithreadedAsync(new Runnable() {
            @Override public void run() {
                Transaction ptx = primaryTransactions.txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE, 2000, 0);
                primaryCache.put(finalBackupKey, 1);
                System.out.println("[deadlock]primaryCache.put(\"key1\", 1);");
                try {
                    barrier.await();
                }
                catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }

                primaryCache.put(finalPrimaryKey, 1);

                System.out.println("[deadlock]primaryCache.put(\"key2\", 1);");

                try {
                    barrier.await();

                    barrier.await();
                }
                catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }

                ptx.commit();
            }
        }, 1);

        Transaction backupTx = backupIgnite.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE, 2000, 0);

        backupCache.put(primaryKey, 2);

        barrier.await();

        barrier.await();

        backupCache.put(backupKey, 2);

        barrier.await();

        backupTx.commit();

        awaitPartitionMapExchange();

        System.out.println("primary key= " + primaryCache.get(primaryKey));
        System.out.println("backup key= " + primaryCache.get(backupKey));
    }

    /**
     *
     */
    static class StoreFactory implements Factory<CacheStore> {
        /** */
        @IgniteInstanceResource
        private Ignite node;

        @Override public CacheStore create() {
            String igniteInstanceName = node.configuration().getIgniteInstanceName();

            if (igniteInstanceName.endsWith("0")) {
                LOCAL_STORE_0.nodeId = node.cluster().localNode().id();

                return LOCAL_STORE_0;
            }
            else if (igniteInstanceName.endsWith("1")) {
                LOCAL_STORE_1.nodeId = node.cluster().localNode().id();

                return LOCAL_STORE_1;
            }
            else if (igniteInstanceName.endsWith("2")) {
                LOCAL_STORE_2.nodeId = node.cluster().localNode().id();

                return LOCAL_STORE_2;
            }
            else
                throw new IllegalArgumentException();
        }
    }
    /**
     *
     */
    @CacheLocalStore
    public static class TestLocalStore<K, V> implements CacheStore<K, IgniteBiTuple<V, ?>> {
        /** */
        private final Map<K, IgniteBiTuple<V, ?>> map = new ConcurrentHashMap<>();

        public UUID nodeId;

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<K, IgniteBiTuple<V, ?>> clo, @Nullable Object... args)
            throws CacheLoaderException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public IgniteBiTuple<V, ?> load(K key) throws CacheLoaderException {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public Map<K, IgniteBiTuple<V, ?>> loadAll(Iterable<? extends K> keys) throws CacheLoaderException {
            Map<K, IgniteBiTuple<V, ?>> res = new HashMap<>();

            for (K key : keys) {
                IgniteBiTuple<V, ?> val = map.get(key);

                if (val != null)
                    res.put(key, val);
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends K, ? extends IgniteBiTuple<V, ?>> entry)
            throws CacheWriterException {
            map.put(entry.getKey(), entry.getValue());

            System.out.println("[txs]Cache write key " + entry.getKey() + " .On node " + nodeId);
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<? extends K, ? extends IgniteBiTuple<V, ?>>> entries)
            throws CacheWriterException {
            for (Cache.Entry<? extends K, ? extends IgniteBiTuple<V, ?>> e : entries) {
                map.put(e.getKey(), e.getValue());

                System.out.println("[txs]Cache write key " + e.getKey() + " .On node " + nodeId);
            }
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            map.remove(key);
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
            for (Object key : keys)
                map.remove(key);
        }

        /**
         * Clear store.
         */
        public void clear(){
            map.clear();
        }
    }
}
