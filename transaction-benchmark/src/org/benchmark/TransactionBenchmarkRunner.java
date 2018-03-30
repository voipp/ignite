package org.benchmark;

import java.io.FileInputStream;
import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.*;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Created by SBT-Kuznetsov-AL on 10.04.2018.
 */
public class TransactionBenchmarkRunner {
    /**
     * @param args Args.
     */
    private static TestConfiguration parseCommandLine(String[] args) throws IOException {
        List<String> parsedArgs = new ArrayList<>();

        /*
         * in *nix arguments, split by space are passed as single args sometimes, so we need to parse them.
         * For isntance, this : "arg1 arg2" can be passed as single argument.
         */
        for (String arg : args)
            parsedArgs.addAll(Arrays.asList(arg.split("\\s+")));

        Options options = new Options();

        Option input = new Option("cfg", true, "test configuration properties file path");
        input.setRequired(true);
        options.addOption(input);

        Option output = new Option("igniteCfg", true, "test ignite configuration file path");
        output.setRequired(true);
        options.addOption(output);

        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, parsedArgs.toArray(new String[parsedArgs.size()]));
        }
        catch (ParseException e) {
            System.out.println(e.getMessage());

            formatter.printHelp("TransactionBenchmarkRunner", options);

            System.exit(1);

            return null;
        }

        TestConfiguration testCfg = new TestConfiguration();

        /* Config. */
        String cfg = cmd.getOptionValue("cfg");
        testCfg.igniteCfg = cmd.getOptionValue("igniteCfg");

        Properties p = new Properties();
        p.load(new FileInputStream(cfg));

        testCfg.srvNum = Integer.parseInt((String)p.get("serverNum"));
        testCfg.keysNum = Integer.parseInt((String)p.get("keysNum"));
        testCfg.threadGrpNum = Integer.parseInt((String)p.get("threadGroupNum"));
        testCfg.cacheName = String.valueOf(p.get("cacheName"));
        testCfg.testTime = Long.parseLong((String)p.get("testTime"));
        testCfg.txConcurrency = TransactionConcurrency.valueOf((String)p.get("txConcurrency"));
        testCfg.txIsolation = TransactionIsolation.valueOf((String)p.get("txIsolation"));
        testCfg.warmupTime = Long.parseLong((String)p.get("warmupTime"));
        testCfg.keysNumbPerGrp = (testCfg.keysNum > testCfg.threadGrpNum ? testCfg.keysNum / testCfg.threadGrpNum : testCfg.keysNum);


        return testCfg;
    }

    /**
     * @param args Args.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        TestConfiguration testCfg = parseCommandLine(args);
        assert testCfg != null;

        Ignite client = Ignition.start(testCfg.igniteCfg);
        //assert client.cluster().localNode().isClient() : "Client node must start test";
        // cluster might be deactivated on start, so activate it.
        client.active(true);

        Thread.sleep(1_000);

        IgniteCache<Integer, CacheValueHolder> cache = client.getOrCreateCache(testCfg.cacheName);
        IgniteTransactions txs = client.transactions();

        IgniteLogger suspendResumeLog = client.log().getLogger("SuspendResumeScenarioLogger");
        IgniteLogger standardLog = client.log().getLogger("StandardScenarioLogger");

        if (client.cluster().forRemotes().forServers().nodes().size() != testCfg.srvNum)
            throw new UnexpectedException("Client failed to connect to cluster");

        suspendResumeLog.debug("Test started." +
            "\nServers number= " + testCfg.srvNum
            + ", keys number= " + testCfg.keysNum
            + ", thread groups number= " + testCfg.threadGrpNum
            + ", starting cache with name= " + testCfg.cacheName
            + ", test time= " + testCfg.testTime
            + ", transaction concurrency= " + testCfg.txConcurrency
            + ", transaction isolation= " + testCfg.txIsolation
            + ", warmup time= " + testCfg.warmupTime
            + ", keys per thread group= " + testCfg.keysNumbPerGrp);

        fillCacheWithInitialValues(client, testCfg);

        for (Integer i = 0; i < testCfg.keysNum; i++)
            assert cache.get(i).val == 0;

        startSuspendResumeScenarioTest(client, cache, txs, suspendResumeLog, testCfg.keysNumbPerGrp, testCfg);

        Thread.sleep(1_000);

        startStandardScenarioTest(client, cache, txs, standardLog, testCfg.keysNumbPerGrp, testCfg);

        cache.removeAll();

        client.close();
    }

    /**
     * Starts standard scenario, without suspend-resume.
     *
     * @param client Client.
     * @param cache Cache.
     * @param txs Txs.
     * @param log Logger.
     * @param keysNumbPerGrp Keys numb per group.
     * @param testCfg Test config.
     */
    private static void startStandardScenarioTest(Ignite client,
        IgniteCache<Integer, CacheValueHolder> cache,
        IgniteTransactions txs,
        IgniteLogger log,
        int keysNumbPerGrp,
        TestConfiguration testCfg)
        throws InterruptedException {
        AtomicInteger keyCntr = new AtomicInteger(0);

        Runnable testStandardScenario = new Runnable() {
            @Override public void run() {
                Integer origin = keyCntr.getAndAdd(keysNumbPerGrp);

                Integer key;
                long txTotalTime;
                long txStartTime;
                Transaction tx = null;

                try {
                    while (true) {
                        key = ThreadLocalRandom.current().nextInt(origin, origin + keysNumbPerGrp);

                        txTotalTime = System.nanoTime();

                        tx = txs.txStart(testCfg.txConcurrency, testCfg.txIsolation);

                        txStartTime = System.nanoTime() - txTotalTime;

                        CacheValueHolder val = cache.get(key);

                        val.val += 2;

                        cache.put(key, val);

                        val.val /= 2;

                        cache.put(key, val);

                        val.val += 3;

                        cache.put(key, val);

                        val.val /= 4;

                        cache.put(key, val);

                        long txCommitTime = System.nanoTime();

                        tx.commit();

                        log.debug(
                            "\nTxTotalTime=" + (System.nanoTime() - txTotalTime) +
                                "\nTxStartTime=" + txStartTime +
                                "\nTxCommitTime=" + (System.nanoTime() - txCommitTime)
                        );
                    }
                }
                finally {
                    if (tx != null)
                        tx.close();
                }
            }
        };

        log.debug("\nStarting standard test scenarios, warmup time= " + testCfg.warmupTime);

        executeScenario(testStandardScenario, testCfg.threadGrpNum, testCfg.testTime + testCfg.warmupTime, client);

        log.debug("Successfully finished standart test scenarios.");
    }

    /**
     * Starts scenario with context switching.
     *
     * @param client Client.
     * @param cache Cache.
     * @param txs Txs.
     * @param log Logger.
     * @param keysNumbPerGrp Keys numb per group.
     * @param testCfg Test config.
     */
    private static void startSuspendResumeScenarioTest(Ignite client,
        IgniteCache<Integer, CacheValueHolder> cache,
        IgniteTransactions txs,
        IgniteLogger log,
        int keysNumbPerGrp,
        TestConfiguration testCfg) throws InterruptedException {
        AtomicInteger keyCntr = new AtomicInteger(0);

        Runnable testSuspendResumeScenario = new Runnable() {
            @Override public void run() {
                Integer key;
                SimpleSingleThreadPool executor = new SimpleSingleThreadPool(cache);

                Transaction tx = null;

                try {
                    Integer origin = keyCntr.getAndAdd(keysNumbPerGrp);

                    long txTotalTime;
                    long txStartTime;
                    long txSuspendResumeTime0;
                    long txSuspendResumeTime1;
                    long txSuspendResumeTime2;

                    while (true) {
                        key = ThreadLocalRandom.current().nextInt(origin, origin + keysNumbPerGrp);

                        txTotalTime = System.nanoTime();

                        tx = txs.txStart(testCfg.txConcurrency, testCfg.txIsolation);

                        txStartTime = System.nanoTime() - txTotalTime;

                        CacheValueHolder val = cache.get(key);

                        val.val += 2;

                        cache.put(key, val);

                        txSuspendResumeTime0 = System.nanoTime();

                        tx.suspend();

                        executor.executeSecondStepTask(tx, key);

                        tx.resume();

                        txSuspendResumeTime1 = System.nanoTime() - executor.timeVar2;

                        txSuspendResumeTime0 = executor.timeVar1 - txSuspendResumeTime0;

                        val = cache.get(key);

                        val.val += 3;

                        cache.put(key, val);

                        txSuspendResumeTime2 = System.nanoTime();

                        tx.suspend();

                        executor.executeFourthStepTask(tx, key);

                        log.debug(
                            "\nTxTotalTime=" + (System.nanoTime() - txTotalTime) +
                                "\nTxStartTime=" + txStartTime +
                                "\nTxCommitTime=" + executor.timeVar2 +
                                "\nTxSuspendResumeTime=" + txSuspendResumeTime0 +
                                "\nTxSuspendResumeTime=" + txSuspendResumeTime1 +
                                "\nTxSuspendResumeTime=" + (executor.timeVar1 - txSuspendResumeTime2)
                        );
                    }
                }
                finally {
                    if (tx != null)
                        tx.close();

                    executor.stopWorker();
                }
            }
        };

        log.debug("Starting suspend/resume test scenarios, warmup time= " + testCfg.warmupTime);

        executeScenario(testSuspendResumeScenario, testCfg.threadGrpNum, testCfg.testTime + testCfg.warmupTime, client);

        log.debug("Successfully finished suspend-resume test scenario.");
    }

    /**
     * Fills cache with initial values by streamer.
     *
     * @param client Client.
     */
    private static void fillCacheWithInitialValues(Ignite client, TestConfiguration testCfg) {
        IgniteDataStreamer<Integer, CacheValueHolder> dataStreamer = client.dataStreamer(testCfg.cacheName);

        dataStreamer.allowOverwrite(true);

        for (Integer i = 0; i < testCfg.keysNum; i++)
            dataStreamer.addData(i, new CacheValueHolder(0));

        dataStreamer.flush();

        dataStreamer.close(false);
    }

    /**
     * @param scenario Scenario to run.
     * @param numbOfThreads Numb of threads to run scenario in.
     * @param timeToWait Time to wait until scenarios will be canceled(in seconds).
     * @param client Client.
     */
    public static void executeScenario(Runnable scenario, int numbOfThreads,
        long timeToWait, Ignite client) throws InterruptedException {
        checkAllTransactionsHaveFinished(client);

        ExecutorService groupsExecutor = Executors.newFixedThreadPool(numbOfThreads);

        for (Integer i = 0; i < numbOfThreads; i++)
            groupsExecutor.submit(scenario);

        groupsExecutor.shutdown();

        groupsExecutor.awaitTermination(timeToWait, TimeUnit.SECONDS);

        groupsExecutor.shutdownNow();

        checkAllTransactionsHaveFinished(client);
    }

    /**
     * Checks whether transactions have finished on all nodes.
     *
     * @param client Client.
     */
    private static void checkAllTransactionsHaveFinished(Ignite client) throws InterruptedException {
        Collection<Boolean> finishTxsBroadcast;

        boolean recheck = true;

        while (recheck) {
            Thread.sleep(500);

            recheck = false;

            finishTxsBroadcast = client.compute(
                client.cluster().forRemotes().forServers()).broadcast(new IgniteCallable<Boolean>() {

                @IgniteInstanceResource
                private Ignite ignite;

                @Override public Boolean call() {
                    GridCacheSharedContext<Object, Object> cctx = ((IgniteKernal)ignite).context().cache().context();

                    return cctx.tm().activeTransactions().isEmpty();
                }
            });

            for (Boolean finishTxsRes : finishTxsBroadcast)
                if (!finishTxsRes) {
                    System.out.println("Some transactions haven't finished yet.");

                    recheck = true;
                }
        }
    }
}
