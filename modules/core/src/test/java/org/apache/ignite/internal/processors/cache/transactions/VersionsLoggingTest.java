package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.transactions.Transaction;

public class VersionsLoggingTest extends IgniteCacheAbstractTest {
    @Override
    protected int gridCount() {
        return 3;
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
    protected NearCacheConfiguration nearConfiguration() {
        return new NearCacheConfiguration();
    }

    public void test1() throws InterruptedException, IgniteCheckedException {
        awaitPartitionMapExchange();

        GridCacheVersion.logVersion = false;

        for (Ignite ignite : G.allGrids()) {
            System.out.println("[txs]grid " + ignite.name() + " \t " + ignite.cluster().localNode().id().getMostSignificantBits());
        }

        assertEquals(1, jcache(0).getConfiguration(CacheConfiguration.class).getBackups());

        Integer key = primaryKey(jcache(1));

        IgniteTransactions txs = ignite(0).transactions();

        awaitPartitionMapExchange();

        GridCacheVersion.logVersion = true;

        Transaction tx = txs.txStart();

        jcache(0).put(key, 1);

        tx.commit();
    }
}
