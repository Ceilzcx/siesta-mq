package com.ceilzcx.siestamq.broker.task;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * @author ceilzcx
 * @since 17/12/2022
 */
public class FutureTaskExt<V> extends FutureTask<V> {
    private final Runnable runnable;

    public FutureTaskExt(final Callable<V> callable) {
        super(callable);
        this.runnable = null;
    }

    public FutureTaskExt(final Runnable runnable, final V result) {
        super(runnable, result);
        this.runnable = runnable;
    }

    public Runnable getRunnable() {
        return runnable;
    }
}
