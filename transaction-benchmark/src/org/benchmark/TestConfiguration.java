package org.benchmark;

import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 *
 */
public class TestConfiguration {
    /** Ignite config. */
    public String igniteCfg;
    /** Server number. */
    public Integer srvNum;
    /** Keys number. */
    public Integer keysNum;
    /** Thread group number. */
    public Integer threadGrpNum;
    /** Cache name. */
    public String cacheName;
    /** Test time. */
    public long testTime;
    /** Tx concurrency. */
    public TransactionConcurrency txConcurrency;
    /** Tx isolation. */
    public TransactionIsolation txIsolation;
    /** Warmup time. */
    public long warmupTime;
    /** Keys numb per group. */
    public int keysNumbPerGrp;
}
