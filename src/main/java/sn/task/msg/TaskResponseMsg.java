package sn.task.msg;


import sn.task.state.TaskState;

/**
 * Created by Sumanth on 20/10/14.
 */
public class TaskResponseMsg  {
    private final long taskId;

    private final TaskState taskState;
    private final TaskResponse taskResponse;

    private String msg;

    public TaskResponseMsg(long taskId, TaskState taskState, final TaskResponse taskResponse) {
        this.taskId = taskId;
        this.taskState = taskState;
        this.taskResponse = taskResponse;
    }


    public TaskResponseMsg(final long taskId,TaskState taskState,final TaskResponse taskResponse, String msg) {
        this.taskId = taskId;
        this.taskState = taskState;
        this.taskResponse =  taskResponse;
        this.msg = msg;
    }

    public long getTaskId() {
        return taskId;
    }

    public TaskState getTaskState() {
        return taskState;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public TaskResponse getTaskResponse() {
        return taskResponse;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskResponseMsg that = (TaskResponseMsg) o;

        if (taskId != that.taskId) return false;
        if (taskState != that.taskState) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (taskId ^ (taskId >>> 32));
        result = 31 * result + (taskState != null ? taskState.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TaskResponseMsg{" +
                "taskId=" + taskId +
                ", taskState=" + taskState +
                ", taskResponse=" + taskResponse +
                ", msg='" + msg + '\'' +
                '}';
    }
}
