package org.apache.ignite.examples.cluster

import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCache
import org.apache.ignite.IgniteLogger
import org.apache.ignite.Ignition
import org.apache.ignite.internal.GridLoggerProxy
import org.apache.ignite.transactions.Transaction
import org.apache.ignite.transactions.TransactionConcurrency
import org.apache.ignite.transactions.TransactionIsolation

import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

//first, parsing input parameters.
def cli = new CliBuilder()

cli.cfg(args: 1, argName: 'cfg', 'test configuration properties file path', required: true)
cli.igniteCfg(args: 1, argName: 'igniteCfg', 'test ignite configuration file path', required: true)

def arguments = cli.parse(args)

Properties p = new Properties()
p.load(new FileInputStream(arguments.cfg))

def serverNum = Integer.parseInt p.get('serverNum') as String
def keysNum = Integer.parseInt p.get('keysNum') as String
def threadGroupNum = Integer.parseInt(p.get('threadGroupNum') as String)
def cacheName = p.get('cacheName')
def testTime = Long.parseLong p.get('testTime') as String
def txConcurrency = TransactionConcurrency.valueOf p.get('txConcurrency') as String
def txIsolation = TransactionIsolation.valueOf p.get('txIsolation') as String
def warmupTime = Integer.parseInt p.get('warmupTime') as String

//starting client.
    Ignite client = Ignition.start(arguments.igniteCfg)

    IgniteCache<String, Integer> cache = client.getOrCreateCache(cacheName)

    def txs = client.transactions()

    IgniteLogger suspendResumeLogger = client.log().getLogger("SuspendResumeScenarioLogger")
    IgniteLogger standardLogger = client.log().getLogger("StandardScenarioLogger")

    suspendResumeLogger.debug "Test started[Servers number= $serverNum, keys number= $keysNum, " +
        "thread groups number= $threadGroupNum, starting cache with name= $cacheName, test time= $testTime, " +
        "transaction concurrency= $txConcurrency, transaction isolation= $txIsolation, warmup time= $warmupTime]"

//((IgniteKernal)client).context().cache().context().exchange().affinityReadyFuture(new AffinityTopologyVersion((long)(serverNum + 1))).get();// ?

    assert client.cluster().forRemotes().forServers().nodes().size() == serverNum: "Client cannot connect to all server nodes"

    int keysNumbPerGroup = (keysNum > threadGroupNum ? keysNum / threadGroupNum : keysNum)

    suspendResumeLogger.debug "Keys per thread group= $keysNumbPerGroup"

    def dataStreamer = client.dataStreamer(cacheName)

//filling cache with initial values.
    keysNum.times {
        dataStreamer.addData(it.toString(), 0)
    }

boolean loggingEnabled = false

dataStreamer.close false

    AtomicInteger keyCntr = new AtomicInteger(0)

    def testSuspendResumeScenario = {
        ExecutorService threadsExecutor = Executors.newFixedThreadPool(1)

        Integer origin = keyCntr.getAndAdd(keysNumbPerGroup)

        String key

        long time, txStartTime

        Callable newThCallable = new Callable() {
            @Override
            Object call() throws Exception {
                tx.resume()

                if (loggingEnabled)
                    suspendResumeLogger.debug("Resumed transaction with time=" + (System.nanoTime() - time))

                res = cache.get(key)

                cache.put(key, res / 2)

                time = System.nanoTime()

                tx.suspend()
            }
        }

        Callable newThCallable2 = new Callable() {
            @Override
            Object call() throws Exception {
                tx.resume()

                if (loggingEnabled)
                    suspendResumeLogger.debug("Resumed transaction with time=" + (System.nanoTime() - time))

                res = cache.get(key)

                cache.put(key, res / 4)

                time = System.nanoTime()

                tx.commit()

                if (loggingEnabled)
                    suspendResumeLogger.debug("Transaction committed with time=" + (System.nanoTime() - time))

                if (loggingEnabled)
                    suspendResumeLogger.debug("Transaction overall time=" + (System.nanoTime() - txStartTime))
            }
        }

        while (true) {
            key = ThreadLocalRandom.current().nextInt(origin, origin + keysNumbPerGroup).toString();

            if (loggingEnabled)
                suspendResumeLogger.debug("Starting transaction, key=" + key)

            time = System.nanoTime()

            txStartTime = time

            Transaction tx = txs.txStart(txConcurrency, txIsolation)

            if (loggingEnabled)
                suspendResumeLogger.debug("Transaction started with time=" + (System.nanoTime() - time))

            int res = cache.get(key)

            cache.put(key, res + 2)

            time = System.nanoTime()

            tx.suspend()

            threadsExecutor.submit(newThCallable).get()

            tx.resume()

            if (loggingEnabled)
                suspendResumeLogger.debug("Resumed transaction with time=" + (System.nanoTime() - time))

            res = cache.get(key)

            cache.put(key, res + 3)

            time = System.nanoTime()

            tx.suspend()

            threadsExecutor.submit(newThCallable2).get()
        }
    }

    ExecutorService groupsExecutor = Executors.newFixedThreadPool(threadGroupNum)

suspendResumeLogger.debug "Starting suspend/resume test scenarios, warmup time= $warmupTime"

    threadGroupNum.times {
        groupsExecutor.submit(testSuspendResumeScenario)
    }

    groupsExecutor.shutdown()

ScheduledExecutorService warmupExecutor = Executors.newScheduledThreadPool(1)

warmupExecutor.schedule({
    suspendResumeLogger.debug 'Warmup has ended, starting the test'

    loggingEnabled = true
}, warmupTime, TimeUnit.SECONDS).get()

warmupExecutor.shutdownNow();

assert !groupsExecutor.awaitTermination(testTime, TimeUnit.SECONDS)

    groupsExecutor.shutdownNow()

    keyCntr.set(0)

    def testStandardScenario = {
        Integer origin = keyCntr.getAndAdd(keysNumbPerGroup)

        while (true) {
            int key = ThreadLocalRandom.current().nextInt(origin, origin + keysNumbPerGroup).toString();

            standardLogger.debug 'Starting transaction, key='.concat(key)

            long time = System.nanoTime()

            long txStartTime = time

            Transaction tx = txs.txStart(txConcurrency, txIsolation)

            standardLogger.debug 'Transaction started with time='.concat(System.nanoTime() - time as String)

            int res = cache.get(key)

            cache.put(key, res + 2)

            res = cache.get(key)

            cache.put(key, res / 2)

            res = cache.get(key)

            cache.put(1, res + 3)

            res = cache.get(key)

            cache.put(1, res / 4)

            time = System.nanoTime()

            tx.commit()

            standardLogger.debug 'Transaction committed with time='.concat(System.nanoTime() - time as String)

            standardLogger.debug 'Transaction overall time='.concat(System.nanoTime() - txStartTime as String)
        }
    }

    groupsExecutor = Executors.newFixedThreadPool(threadGroupNum)

standardLogger.debug "Starting standard test scenarios, warmup time= $warmupTime"

    threadGroupNum.times {
        groupsExecutor.submit(testStandardScenario)
    }

    groupsExecutor.shutdown()

warmupExecutor = Executors.newScheduledThreadPool(1)

warmupExecutor.schedule({
    standardLogger.debug 'Warmup has ended, starting the test'

    loggingEnabled = true
}, warmupTime, TimeUnit.SECONDS).get()

warmupExecutor.shutdownNow();

assert !groupsExecutor.awaitTermination(testTime, TimeUnit.SECONDS)

    groupsExecutor.shutdownNow()

    standardLogger.debug "Successfully finished test scenarios."

    cache.removeAll()

    client.close()
