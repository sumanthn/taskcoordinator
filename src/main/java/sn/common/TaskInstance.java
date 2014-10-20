package sn.common;

/**
 * Created by Sumanth on 17/10/14.
 */
public class TaskInstance {

    private long id;
    private TaskType taskType;

    private TaskParameters taskParameters;

    public TaskInstance(long id, TaskType taskType) {
        this.id = id;
        this.taskType = taskType;
    }

    public long getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(TaskType taskType) {
        this.taskType = taskType;
    }

    public TaskParameters getTaskParameters() {
        return taskParameters;
    }

    public void setTaskParameters(TaskParameters taskParameters) {
        this.taskParameters = taskParameters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskInstance that = (TaskInstance) o;

        if (id != that.id) return false;
        if (taskType != that.taskType) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (taskType != null ? taskType.hashCode() : 0);
        return result;
    }

    public static void main(String [] args){

        TaskInstance taskInstance = new TaskInstance(10, TaskType.GENIE_MR);
        final HttpServiceParams serviceParams = new HttpServiceParams("http", "localhost", 8080, "/genie/v0/jobs");
        GenieMRTaskParameters mrTaskParameters = new GenieMRTaskParameters(serviceParams, "Sumanth", "hadoop",
                "jar /tmp/hdptest.jar cruncher.TxnOperations /txndatain /txnout5",
                "file:///tmp/hdptest.jar",10,3);
        taskInstance.setTaskParameters(mrTaskParameters);
    }
}
