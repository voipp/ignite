package org.apache.ignite.internal.util;

import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class StripedExecutorTest extends GridCommonAbstractTest {

    private StripedExecutor stripedExecSvc;

    public void testCompletedTasks() throws Exception {
        before(3);
        stripedExecSvc.execute(0, new TestRunnable());
        stripedExecSvc.execute(1, new TestRunnable());
        sleepASec();
        assertEquals(2, stripedExecSvc.completedTasks());
        after();
    }

    public void testStripesCompletedTasks() throws Exception {
        before(3);
        stripedExecSvc.execute(0, new TestRunnable());
        stripedExecSvc.execute(1, new TestRunnable());
        sleepASec();
        long[] completedTaks = stripedExecSvc.stripesCompletedTasks();
        assertEquals(1, completedTaks[0]);
        assertEquals(1, completedTaks[1]);
        assertEquals(0, completedTaks[2]);
        after();
    }

    public void testStripesActiveStatuses() throws Exception {
        before(3);
        stripedExecSvc.execute(0, new TestRunnable());
        stripedExecSvc.execute(1, new TestRunnable(true));
        sleepASec();
        boolean[] statuses = stripedExecSvc.stripesActiveStatuses();
        assertFalse(statuses[0]);
        assertTrue(statuses[1]);
        assertFalse(statuses[0]);
        after();
    }

    public void testActiveStripesCount() throws Exception {
        before(3);
        stripedExecSvc.execute(0, new TestRunnable());
        stripedExecSvc.execute(1, new TestRunnable(true));
        sleepASec();
        assertEquals(1, stripedExecSvc.activeStripesCount());
        after();
    }

    public void testStripesQueueSizes() throws Exception {
        before(3);
        stripedExecSvc.execute(0, new TestRunnable());
        stripedExecSvc.execute(0, new TestRunnable(true));
        stripedExecSvc.execute(0, new TestRunnable(true));
        stripedExecSvc.execute(1, new TestRunnable(true));
        stripedExecSvc.execute(1, new TestRunnable(true));
        stripedExecSvc.execute(1, new TestRunnable(true));
        sleepASec();
        int[] queueSizes = stripedExecSvc.stripesQueueSizes();
        assertEquals(1, queueSizes[0]);
        assertEquals(2, queueSizes[1]);
        assertEquals(0, queueSizes[2]);
        after();
    }

    public void testQueueSize() throws Exception {
        before(1);
        stripedExecSvc.execute(1, new TestRunnable());
        stripedExecSvc.execute(1, new TestRunnable(true));
        stripedExecSvc.execute(1, new TestRunnable(true));
        sleepASec();
        assertEquals(1, stripedExecSvc.queueSize());
        after();
    }

    private final class TestRunnable implements Runnable {

        private boolean infinitely;

        public TestRunnable() {
            super();
        }

        public TestRunnable(boolean infinitely) {
            this.infinitely = infinitely;
        }

        @Override
        public void run() {
            try {
                while (infinitely) {
                    Thread.sleep(10000);
                }
            } catch (InterruptedException e) {
                info("Got interrupted exception while sleeping: " + e);
            }
        }
    }

    private void sleepASec() throws InterruptedException {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            info("Got interrupted exception while sleeping: " + e);
        }
    }

    public void before(int stripesNumber) throws Exception {
        stripedExecSvc = new StripedExecutor(stripesNumber, "foo name", "pool name", new JavaLogger());
    }

    public void after() {
        stripedExecSvc.shutdown();
    }
}