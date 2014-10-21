package sn.jobmanager;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import sn.common.JobInstance;
import sn.common.def.JobDef;
import sn.task.JobCoordinator;
import sn.task.msg.JobCommand;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by Sumanth on 21/10/14.
 */
public class JobManager {

    private Logger logger =Logger.getLogger(JobManager.class.getName());
    ActorSystem _system = ActorSystem.create("JobManager");
    private Map<Long,ActorRef> jobCoordinatorMap = new HashMap<Long, ActorRef>();

    private static JobManager ourInstance = new JobManager();

    public static JobManager getInstance() {
        return ourInstance;
    }

    private JobManager() {
    }

    public void launchJob(final JobDef jobDef){

        //make instance and launch the job
        JobInstance jobInstance =
                jobDef.buildJobInstance();
        logger.info("Launching job " + jobDef.getName() + " instance id " + jobInstance.getId());


        ActorRef cordRef = _system.actorOf(JobCoordinator.create(jobInstance),"JobCoordinator-"+jobInstance.getId());
        jobCoordinatorMap.put(jobInstance.getId(),cordRef);
        cordRef.tell(JobCommand.INIT,null);
        cordRef.tell(JobCommand.RUN,null);
    }
}

