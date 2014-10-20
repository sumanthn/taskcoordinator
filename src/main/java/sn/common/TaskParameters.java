package sn.common;

/**
 * Created by Sumanth on 17/10/14.
 */
public abstract class TaskParameters {

    protected int timeout;
    protected int retry;

    protected TaskParameters(int timeout, int retry) {
        this.timeout = timeout;
        this.retry = retry;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getRetry() {
        return retry;
    }
}
