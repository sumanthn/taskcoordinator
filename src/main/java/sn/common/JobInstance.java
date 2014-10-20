package sn.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Sumanth on 17/10/14.
 */
public class JobInstance {
    private static final int DEFAULT_JOB_PRIORITY=3;
    private final long id;
    private String jobDefId;
    private int priority = DEFAULT_JOB_PRIORITY ;

    private List<TaskInstance> taskInstances = new ArrayList<TaskInstance>();

    public JobInstance(){
        this.id = GlobalIdAssigner.getInstance().getId();
    }
    public JobInstance(final List<TaskInstance> taskInstances){
        this.id = GlobalIdAssigner.getInstance().getId();

        this.taskInstances = taskInstances;
    }



    public static int getDefaultJobPriority() {
        return DEFAULT_JOB_PRIORITY;
    }

    public long getId() {
        return id;
    }

    public String getJobDefId() {
        return jobDefId;
    }

    public void setJobDefId(String jobDefId) {
        this.jobDefId = jobDefId;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public List<TaskInstance> getTaskInstances() {
        return taskInstances;
    }

    public void setTaskInstances(List<TaskInstance> taskInstances) {
        this.taskInstances = taskInstances;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JobInstance that = (JobInstance) o;

        if (id != that.id) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }


    public static void main(String [] args){
        JobInstance jobInstance = new JobInstance();
        //should be UUID of the job
        jobInstance.setJobDefId("GenieComboRunner");

        TaskInstance genieTask =  new TaskInstance(GlobalIdAssigner.getInstance().getId(),TaskType.GENIE_MR);
        final HttpServiceParams serviceParams = new HttpServiceParams("http", "localhost", 8080, "/genie/v0/jobs");
        GenieMRTaskParameters mrTaskParameters = new GenieMRTaskParameters(serviceParams, "Sumanth", "hadoop",
                "jar /tmp/hdptest.jar cruncher.TxnOperations /txndatain /txnout5",
                "file:///tmp/hdptest.jar",10,3);
        genieTask.setTaskParameters(mrTaskParameters);

        CommandTaskParameters cmdTaskParams =
                new CommandTaskParameters("python","/Users/Sumanth/scripts/tst1.py",60,2);

        TaskInstance commandTask = new TaskInstance(GlobalIdAssigner.getInstance().getId(),TaskType.COMMAND);
        commandTask.setTaskParameters(cmdTaskParams);

        jobInstance.taskInstances.add(genieTask);
        jobInstance.taskInstances.add(commandTask);









    }
}
