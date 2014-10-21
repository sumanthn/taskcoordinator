package sn.common.def;

import sn.common.JobInstance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Sumanth on 20/10/14.
 */
public class JobDef extends AbstractDef {

    private String projectName;
    //to be added in DB
    private String description;

    //to use reflection to convert params
    //global params
    private Map<String,String> params = new HashMap<String, String>();

    //sequential tasks
    //here dependency is assumed to be serial

    private List<TaskDef> taskDefinition = new ArrayList<TaskDef>();

    public JobDef(String uuid, String projectName) {
        super(uuid);
        this.projectName = projectName;
    }

    public JobDef(String uuid, String name, String projectName) {
        super(uuid, name);
        this.projectName = projectName;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    public List<TaskDef> getTaskDefinition() {
        return taskDefinition;
    }

    public void setTaskDefinition(List<TaskDef> taskDefinition) {
        this.taskDefinition = taskDefinition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JobDef that = (JobDef) o;

        if (!uuid.equals(that.uuid)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }

    public synchronized JobInstance buildJobInstance(){
        JobInstance jobInstance = new JobInstance();
        jobInstance.setJobDefId(getUuid());


        //for each build the task instance and set it
        for(TaskDef taskDef : taskDefinition){
            jobInstance.getTaskInstances().add(taskDef.buildTaskInstance());
        }

        return jobInstance;
    }

}


