package sn.jobmanager;

import sn.common.CommandTaskParameters;
import sn.common.GenieMRTaskParameters;
import sn.common.HttpServiceParams;
import sn.common.def.CLITaskDef;
import sn.common.def.GenieMRTaskDef;
import sn.common.def.JobDef;
import sn.common.def.TaskDef;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by Sumanth on 21/10/14.
 */
public class StartupManager {

    private static StartupManager ourInstance = new StartupManager();

    public static StartupManager getInstance() {
        return ourInstance;
    }

    private StartupManager() {
    }

    public static void main(String [] args){
    /*
        for(int i=0;i < 5;i++)
        System.out.println(UUID.randomUUID().toString());*/

        JobStore.getInstance().init();
        ScheduleManager.getInstance().init();
/*

        JobDef jobDef = new JobDef("eb622daf-fd75-4555-a61d-b4f657ba0ad5","CleanupTrail","Valyx");
        jobDef.setDescription("Sample CLI worker");
        List<TaskDef> taskDefs = new ArrayList<TaskDef>();
        TaskDef taskDef = new CLITaskDef("d02f6cf9-992b-42a0-a1fb-fc604f0ca036","Mob-Cleaner","python","/Users/Sumanth/scripts/tst1.py");

        taskDefs.add(taskDef);
        jobDef.setTaskDefinition(taskDefs);
        final String cronExpression = "0 0/2 * * * ? *";
*/

        final String cronExpression = "0 0/5 * * * ? *";
        JobDef jobDef = buildJobDef();

        ScheduleManager.getInstance().loadJobInstance(jobDef,cronExpression);
    }

    public static JobDef buildJobDef(){
        JobDef jobDef = new JobDef("eb622daf-fd75-4555-a61d-b4f657ba0ad5","ServerFaultyIpAggregator","AnalyticsPipe");
        jobDef.setDescription("Sample CLI worker");
        List<TaskDef> taskDefs = new ArrayList<TaskDef>();
        TaskDef cleanupTask = new CLITaskDef("d02f6cf9-992b-42a0-a1fb-fc604f0ca036","InputStageTask",
                "python","/Users/Sumanth/scripts/cleandir.py");
        taskDefs.add(cleanupTask);

        final HttpServiceParams serviceParams = new HttpServiceParams("http", "localhost", 8080, "/genie/v0/jobs");
        GenieMRTaskDef serverIpAggTask = new GenieMRTaskDef("f2cdad4b-091d-4cb7-aa31-1c9a32caa03d","FaultyServerIpAgg_MRTask",serviceParams,
                "Sumanth","hadoop");
        serverIpAggTask.setTimeout(1200);
        serverIpAggTask.setHadoopCommand("jar /tmp/hdptest.jar cruncher.TxnOperations /txndatain /faultyserveripdataout");
        serverIpAggTask.setHadoopFiles("file:///tmp/hdptest.jar");

        taskDefs.add(serverIpAggTask);

        TaskDef stageResTask = new CLITaskDef("c5659a6d-00f8-4686-a882-a72f8b3f723f","StageResTask",
                "python","/Users/Sumanth/scripts/stageresults.py");


        taskDefs.add(stageResTask);

        jobDef.setTaskDefinition(taskDefs);

        return jobDef;
    }
}
