package sn.common.def;

import sn.common.TaskInstance;

/**
 * Created by Sumanth on 20/10/14.
 */
public abstract class TaskDef extends AbstractDef{

    protected int timeout=60;
    protected int retry=3;
    public TaskDef(String uuid) {
        super(uuid);
    }

    public TaskDef(String uuid,  String name) {
        super(uuid, name);
    }


    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getRetry() {
        return retry;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskDef that = (TaskDef) o;

        if (!uuid.equals(that.uuid)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }

    public abstract TaskInstance buildTaskInstance();
}
