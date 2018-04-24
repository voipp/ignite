package org.benchmark;

import java.io.FileInputStream;
import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.*;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.lang.GridTuple6;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.log4j.Logger;

import static org.apache.ignite.transactions.TransactionState.*;
import static org.benchmark.TestConfiguration.*;

/**
 * Created by SBT-Kuznetsov-AL on 10.04.2018.
 */
public class TransactionBenchmarkRunner {
    /**
     * @param args Args.
     */
    public static void main(String[] args) throws IOException {
        TestConfiguration testCfg = parseCommandLine(args);
        assert testCfg != null;

        IgniteCache<Integer, CacheValueHolder> cache;

        try (Ignite client = Ignition.start(testCfg.igniteCfg)) {
            Logger suspendResumeScenarioLog = Logger.getLogger("SuspendResumeScenarioLogger");
            Logger standardScenarioLog = Logger.getLogger("StandardScenarioLogger");

            // cluster might be deactivated on start, so activate it.
            client.active(true);

            if (client.cluster().forRemotes().forServers().nodes().size() != testCfg.srvNum)
                throw new UnexpectedException("Client failed to connect to cluster");

            int backups = client.cache(testCfg.cacheName).getConfiguration(CacheConfiguration.class).getBackups();

            if (backups > testCfg.srvNum)
                throw new IllegalArgumentException("Incorrect number of backup nodes=" + backups
                    + ". Must be less than number of server nodes= " + testCfg.srvNum);

            assert client.cluster().localNode().isClient() : "Client node must start test";

            Thread.sleep(1_000);

            cache = client.getOrCreateCache(testCfg.cacheName);

            cache.clear();

            fillCacheWithInitialValues(client, testCfg);

            for (Integer i = 0; i < testCfg.keysNum; i++)
                assert cache.get(i).val == 0;

            suspendResumeScenarioLog.debug(
                "Starting test." +
                    "\nServers number= " + testCfg.srvNum
                    + "\nkeys number= " + testCfg.keysNum
                    + "\nthread groups number= " + testCfg.threadGrpNum
                    + "\nstarting cache with name= " + testCfg.cacheName
                    + "\ntest time= " + testCfg.testTime
                    + "\ntransaction concurrency= " + testCfg.txConcurrency
                    + "\ntransaction isolation= " + testCfg.txIsolation
                    + "\nwarmup time= " + testCfg.warmupTime
                    + "\nkeys per thread group= " + testCfg.keysNumbPerGrp);

            runSuspendResumeScenarioTest(client, cache, suspendResumeScenarioLog, testCfg);

            Thread.sleep(1_000);

            runStandardScenarioTest(client, cache, standardScenarioLog, testCfg);

            if (cache != null)
                cache.clear();
        }
        catch (InterruptedException e) {
            //No-op.
        }
    }

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

        testCfg.srvNum = Integer.parseInt((String)p.get("server-num"));
        testCfg.keysNum = Integer.parseInt((String)p.get("keys-num"));
        testCfg.threadGrpNum = Integer.parseInt((String)p.get("thread-group-num"));
        testCfg.cacheName = String.valueOf(p.get("cache-name"));
        testCfg.testTime = Long.parseLong((String)p.get("test-time"));
        testCfg.txConcurrency = TransactionConcurrency.valueOf((String)p.get("tx-concurrency"));
        testCfg.txIsolation = TransactionIsolation.valueOf((String)p.get("tx-isolation"));
        testCfg.warmupTime = Long.parseLong((String)p.get("warmup-time"));
        testCfg.keysNumbPerGrp = (testCfg.keysNum > testCfg.threadGrpNum ? testCfg.keysNum / testCfg.threadGrpNum : testCfg.keysNum);
        testCfg.queueSize = p.containsKey("queue-size") ? Integer.parseInt((String)p.get("queue-size")) : DEFAULT_QUEUE_SIZE;

        return testCfg;
    }

    /**
     * Starts standard scenario, without suspend-resume.
     *
     * @param client Client.
     * @param cache Cache.
     * @param log Logger.
     * @param testCfg Test config.
     */
    private static void runStandardScenarioTest(Ignite client,
        IgniteCache<Integer, CacheValueHolder> cache,
        Logger log,
        TestConfiguration testCfg)
        throws InterruptedException {
        int keyCntr = 0;

        log.debug(
            "Starting standard test scenario.\nTotal test time=" + (testCfg.warmupTime + testCfg.testTime)
                + "(sec.) \nwarmup time= " + testCfg.warmupTime + "(sec.)");

        ExecutorService groupsExecutor = Executors.newFixedThreadPool(testCfg.threadGrpNum);

        for (Integer i = 0; i < testCfg.threadGrpNum; i++) {
            groupsExecutor.submit(new StandardScenario(keyCntr, testCfg, cache, log, client));

            keyCntr +=testCfg.keysNumbPerGrp;
        }

        groupsExecutor.shutdown();

        groupsExecutor.awaitTermination(testCfg.testTime + testCfg.warmupTime, TimeUnit.SECONDS);

        groupsExecutor.shutdownNow();

        checkAllTransactionsHaveFinished(client);

        log.debug("Successfully finished standard test scenarios.");
    }

    /**
     * Starts scenario with context switching.
     *
     * @param client Client.
     * @param cache Cache.
     * @param log Logger.
     * @param testCfg Test config.
     */
    private static void runSuspendResumeScenarioTest(
        Ignite client,
        IgniteCache<Integer, CacheValueHolder> cache,
        Logger log,
        TestConfiguration testCfg) throws InterruptedException {
        checkAllTransactionsHaveFinished(client);

        log.debug(
            "Starting suspend/resume test scenario.\nTotal test time=" + (testCfg.warmupTime + testCfg.testTime)
            + "(sec.) \nwarmup time= " + testCfg.warmupTime + "(sec.)");

        ExecutorService step1Executor = Executors.newFixedThreadPool(testCfg.threadGrpNum);

        LinkedBlockingQueue<GridTuple6<Transaction, Integer, Long, Long, Long, Long>> step2Queue = new LinkedBlockingQueue<>(testCfg.queueSize);
        ExecutorService step2Executor = Executors.newFixedThreadPool(testCfg.threadGrpNum);

        LinkedBlockingQueue<GridTuple6<Transaction, Integer, Long, Long, Long, Long>> step3Queue = new LinkedBlockingQueue<>(testCfg.queueSize);
        ExecutorService step3Executor = Executors.newFixedThreadPool(testCfg.threadGrpNum);

        LinkedBlockingQueue<GridTuple6<Transaction, Integer, Long, Long, Long, Long>> step4Queue = new LinkedBlockingQueue<>(testCfg.queueSize);
        ExecutorService step4Executor = Executors.newFixedThreadPool(testCfg.threadGrpNum);

        List<Task> tasks = new ArrayList<>();

        int keyCntr = 0;

        for (Integer i = 0; i < testCfg.threadGrpNum; i++) {
            Step4Task task4 = new Step4Task(step4Queue, cache, log);
            ((ExecutorService)step4Executor).submit(task4);
            tasks.add(task4);

            Step3Task task3 = new Step3Task(step3Queue, step4Queue, cache, log);
            ((ExecutorService)step3Executor).submit(task3);
            tasks.add(task3);

            Step2Task task2 = new Step2Task(step2Queue, step3Queue, cache, log);
            ((ExecutorService)step2Executor).submit(task2);
            tasks.add(task2);

            Step1Task task1 = new Step1Task(step2Queue, cache, log, client, testCfg, keyCntr, testCfg.keysNumbPerGrp);
            ((ExecutorService)step1Executor).submit(task1);
            tasks.add(task1);

            keyCntr += testCfg.keysNumbPerGrp;
        }

        step4Executor.shutdown();
        step3Executor.shutdown();
        step2Executor.shutdown();
        step1Executor.shutdown();

        step4Executor.awaitTermination(testCfg.testTime + testCfg.warmupTime, TimeUnit.SECONDS);

        tasks.forEach(Task::stop);

        Thread.sleep(1_000);

        step4Executor.shutdownNow();
        step3Executor.shutdownNow();
        step2Executor.shutdownNow();
        step1Executor.shutdownNow();

        step2Queue.forEach( entry -> {
            Transaction transaction = entry.get1();

            if (SUSPENDED.equals(transaction.state()))
                transaction.resume();

            transaction.close();
        });
        step3Queue.forEach( entry -> {
            Transaction transaction = entry.get1();

            if (SUSPENDED.equals(transaction.state()))
                transaction.resume();

            transaction.close();
        });
        step4Queue.forEach( entry -> {
            Transaction transaction = entry.get1();

            if (SUSPENDED.equals(transaction.state()))
                transaction.resume();

            transaction.close();
        });

        checkAllTransactionsHaveFinished(client);

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
