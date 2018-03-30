package org.apache.ignite.examples.cluster

import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCache
import org.apache.ignite.IgniteLogger
import org.apache.ignite.Ignition
import org.apache.ignite.transactions.Transaction
import org.apache.ignite.transactions.TransactionConcurrency
import org.apache.ignite.transactions.TransactionIsolation

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

//first, parsing input parameters.
def cli = new CliBuilder()

cli.cfg(args: 1, argName: 'cfg', 'test configuration properties file path', required: true)
cli.igniteCfg(args: 1, argName: 'igniteCfg', 'test ignite configuration file path', required: true)
cli.testMode(args: 1, argName: 'testMode', 'whether running test mode(true) or calculating result(false)', required: true)
cli.testResults(args: 1, argName: 'testResults', 'path to test results', required: false)

def arguments = cli.parse(args)

Properties p = new Properties()
p.load(new FileInputStream(arguments.cfg))

def serverNum = Integer.parseInt p.get('serverNum')
def keysNum = Integer.parseInt p.get('keysNum')
def threadGroupNum = Integer.parseInt p.get('threadGroupNum')
def cacheName = p.get('cacheName')
def testTime = Long.parseLong p.get('testTime')
def txConcurrency = TransactionConcurrency.valueOf p.get('txConcurrency')
def txIsolation = TransactionIsolation.valueOf p.get('txIsolation')

def testMode = Boolean.parseBoolean arguments.testMode

//starting client.
    Ignite client = Ignition.start(arguments.igniteCfg)

    IgniteCache<String, Integer> cache = client.getOrCreateCache(cacheName)

    def txs = client.transactions()

    IgniteLogger suspendResumeLogger = client.log().getLogger("SuspendResumeScenarioLogger")
    IgniteLogger standardLogger = client.log().getLogger("StandardScenarioLogger")

    suspendResumeLogger.debug "Test started[Servers number= $serverNum, keys number= $keysNum, " +
        "thread groups number= $threadGroupNum, starting cache with name= $cacheName, test time= $testTime, " +
        "transaction concurrency= $txConcurrency, transaction isolation= $txIsolation]"

//((IgniteKernal)client).context().cache().context().exchange().affinityReadyFuture(new AffinityTopologyVersion((long)(serverNum + 1))).get();// ?

    assert client.cluster().forRemotes().forServers().nodes().size() == serverNum: "Client cannot connect to all server nodes"

    int keysNumbPerGroup = (keysNum > threadGroupNum ? keysNum / threadGroupNum : keysNum)

    suspendResumeLogger.debug "Keys per thread group= $keysNumbPerGroup"

    def dataStreamer = client.dataStreamer(cacheName)

//filling cache with initial values.
    keysNum.times {
        suspendResumeLogger.debug "putting key $it into cache"

        dataStreamer.addData(it.toString(), 0)
    }

    dataStreamer.close false

    AtomicInteger keyCntr = new AtomicInteger(0)

    def testSuspendResumeScenario = {
        ExecutorService threadsExecutor = Executors.newFixedThreadPool(1)

        Integer origin = keyCntr.getAndAdd(keysNumbPerGroup)

        while (true) {
            int key = ThreadLocalRandom.current().nextInt(origin, origin + keysNumbPerGroup).toString();

            suspendResumeLogger.debug("Starting transaction, key=".concat(key))

            long time = System.nanoTime()

            long txStartTime = time

            Transaction tx = txs.txStart(txConcurrency, txIsolation)

            suspendResumeLogger.debug 'Transaction started with time='.concat(System.nanoTime() - time as String)

            int res = cache.get(key)

            cache.put(key, res + 2)

            time = System.nanoTime()

            tx.suspend()

            threadsExecutor.submit({
                tx.resume()

                suspendResumeLogger.debug 'Resumed transaction with time='.concat(System.nanoTime() - time as String)

                res = cache.get(key)

                cache.put(key, res / 2)

                time = System.nanoTime()

                tx.suspend()
            }).get()

            tx.resume()

            suspendResumeLogger.debug 'Resumed transaction with time='.concat(System.nanoTime() - time as String)

            res = cache.get(key)

            cache.put(key, res + 3)

            time = System.nanoTime()

            tx.suspend()

            threadsExecutor.submit({
                tx.resume()

                suspendResumeLogger.debug 'Resumed transaction with time='.concat(System.nanoTime() - time as String)

                res = cache.get(key)

                cache.put(key, res / 4)

                time = System.nanoTime()

                tx.commit()

                suspendResumeLogger.debug 'Transaction committed with time='.concat(System.nanoTime() - time as String)

                suspendResumeLogger.debug 'Transaction overall time='.concat(System.nanoTime() - txStartTime as String)
            }).get()
        }
    }

    ExecutorService groupsExecutor = Executors.newFixedThreadPool(threadGroupNum)

    suspendResumeLogger.debug 'Starting suspend/resume test scenarios'

    threadGroupNum.times {
        groupsExecutor.submit(testSuspendResumeScenario)
    }

    groupsExecutor.shutdown()

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

    standardLogger.debug 'Starting standard test scenarios'

    threadGroupNum.times {
        groupsExecutor.submit(testStandardScenario)
    }

    groupsExecutor.shutdown()

    assert !groupsExecutor.awaitTermination(testTime, TimeUnit.SECONDS)

    groupsExecutor.shutdownNow()

    standardLogger.debug "Successfully finished test scenarios."

    cache.removeAll()

    client.close()
