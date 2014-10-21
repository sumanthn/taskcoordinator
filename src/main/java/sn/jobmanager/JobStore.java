package sn.jobmanager;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import sn.common.CommandTaskParameters;
import sn.common.JobInstance;
import sn.common.def.CLITaskDef;
import sn.common.def.JobDef;
import sn.common.def.TaskDef;
import sn.task.JobCoordinator;
import sn.task.msg.JobCommand;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Created by Sumanth on 20/10/14.
 */
public class JobStore {


    private Logger logger = Logger.getLogger(JobStore.class.getName());
    private static JobStore ourInstance = new JobStore();

    ActorSystem _system = ActorSystem.create("TaskCoordinator");

    public static JobStore getInstance() {
        return ourInstance;
    }

    private JobStore() {
    }

    public void init(){
        //for now upload jobs manually


       /* CommandTaskParameters cleanDirTaskParams =
                new CommandTaskParameters("python","/Users/Sumanth/scripts/cleandir.py",60,2);


        JobDef jobDef = new JobDef("eb622daf-fd75-4555-a61d-b4f657ba0ad5","Valyx");
        jobDef.setDescription("Sample CLI worker");
        List<TaskDef> taskDefs = new ArrayList<TaskDef>();
        TaskDef taskDef = new CLITaskDef("d02f6cf9-992b-42a0-a1fb-fc604f0ca036","Mob-Cleaner","python","/Users/Sumanth/scripts/tst1.py");

        taskDefs.add(taskDef);
        jobDef.setTaskDefinition(taskDefs);

        JobInstance jobInstance =
                jobDef.buildJobInstance();

        System.out.println("Built job instance " + jobInstance.getTaskInstances().get(0).getTaskType());


        ActorRef cordRef = _system.actorOf(JobCoordinator.create(jobInstance),"JobCoordinator-"+jobInstance.getId());
        cordRef.tell(JobCommand.INIT,null);
        cordRef.tell(JobCommand.RUN,null);*/

    }

    public static void main(String [] args){
        for(int i=0;i < 10;i++)
            System.out.println(UUID.randomUUID().toString());

    }


}
