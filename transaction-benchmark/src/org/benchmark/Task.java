package org.benchmark;

import org.apache.log4j.Logger;

/** */
public abstract class Task implements Runnable {
    /** Logger. */
    protected final Logger log;
    /** Active. */
    protected boolean active = true;

    /**
     * @param log Logger.
     */
    public Task(Logger log) {
        this.log = log;
    }

    /**
     * Stops runnable;
     */
    public void stop() {
        active = false;
    }
}
