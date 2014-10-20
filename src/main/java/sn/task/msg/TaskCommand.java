package sn.task.msg;

/**
 * Created by Sumanth on 16/10/14.
 */
public enum TaskCommand {
    INIT,
    RUN,
    SUBMIT,
    STATUS,
    CANCEL,
    DESTROY //kill the child actor
}
