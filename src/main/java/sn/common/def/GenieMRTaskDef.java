package sn.common.def;

import sn.common.*;

/**
 * Created by Sumanth on 20/10/14.
 */
public class GenieMRTaskDef extends TaskDef {


    private final HttpServiceParams httpServiceParams;
    private final String userName;
    private final String groupName;

    private String hadoopCommand;
    private String hadoopFiles;

    public GenieMRTaskDef(String uuid, HttpServiceParams httpServiceParams, String userName, String groupName) {
        super(uuid);
        this.httpServiceParams = httpServiceParams;
        this.userName = userName;
        this.groupName = groupName;
    }

    public GenieMRTaskDef(String uuid, String name, HttpServiceParams httpServiceParams, String userName, String groupName) {
        super(uuid, name);
        this.httpServiceParams = httpServiceParams;
        this.userName = userName;
        this.groupName = groupName;
    }

    public HttpServiceParams getHttpServiceParams() {
        return httpServiceParams;
    }

    public String getUserName() {
        return userName;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getHadoopCommand() {
        return hadoopCommand;
    }

    public void setHadoopCommand(String hadoopCommand) {
        this.hadoopCommand = hadoopCommand;
    }

    public String getHadoopFiles() {
        return hadoopFiles;
    }

    public void setHadoopFiles(String hadoopFiles) {
        this.hadoopFiles = hadoopFiles;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GenieMRTaskDef that = (GenieMRTaskDef) o;

        if (!uuid.equals(that.uuid)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }


    @Override
    public synchronized TaskInstance buildTaskInstance() {

        TaskInstance taskInstance = new TaskInstance(GlobalIdAssigner.getInstance().getId(), TaskType.GENIE_MR);
        taskInstance.setTaskUuid(getUuid());
        GenieMRTaskParameters taskParameters = new GenieMRTaskParameters(httpServiceParams,userName,groupName,
                hadoopCommand,hadoopFiles,timeout,retry);
        taskInstance.setTaskParameters(taskParameters);
        return taskInstance;
        /**
         * Sample
         * final HttpServiceParams serviceParams = new HttpServiceParams("http", "localhost", 8080, "/genie/v0/jobs");
         GenieMRTaskParameters mrTaskParameters = new GenieMRTaskParameters(serviceParams, "Sumanth", "hadoop",
         "jar /tmp/hdptest.jar cruncher.TxnOperations /txndatain /txnout5",
         "file:///tmp/hdptest.jar",10,3);
         taskInstance.setTaskParameters(mrTaskParameters);
         */
    }


}
