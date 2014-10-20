package sn.task;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.concurrent.duration.Duration;
import sn.common.*;
import sn.task.msg.JobCommand;
import sn.task.msg.TaskCommand;
import sn.task.msg.TaskResponseMsg;
import sn.task.msg.TickMsg;
import sn.task.state.JobStatus;
import sn.task.state.TaskState;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sumanth on 20/10/14.
 */
public class JobCoordinator extends UntypedActor {

    private LoggingAdapter logger = Logging.getLogger(getContext().system(), this);


    private JobInstance jobInstance;
    private Map<Long,TaskState> taskStates ;
    private boolean reactOnTick;

    private JobStatus jobStatus ;
    //this could be a list in case tasks are executed in parallel
    private long curTaskInstanceId;
    private ActorRef workerTaskRef;
    private int taskNodeIdx;


    public JobCoordinator(JobInstance jobInstance) {
        this.jobInstance = jobInstance;
        taskStates= new LinkedHashMap<Long,TaskState>(jobInstance.getTaskInstances().size());
        jobStatus = JobStatus.NOT_READY;
        reactOnTick=false;
        //currently tasks are serial, a simple LL would do
        taskNodeIdx = 0;

    }

    public static Props create(final JobInstance jobInstance){
        return Props.create(JobCoordinator.class,jobInstance);
    }

    private TaskInstance getNextTask(){

        System.out.println("Get next task now " +  taskNodeIdx);
        if (taskNodeIdx < jobInstance.getTaskInstances().size()){

            TaskInstance runInstance =
                    jobInstance.getTaskInstances().get(taskNodeIdx);
            taskNodeIdx++;
            return runInstance;
        }

        return null;



    }


    @Override
    public void onReceive(Object msg) throws Exception {


        if (msg instanceof JobCommand){
            switch((JobCommand)msg){

                case INIT:
                    for(TaskInstance taskInstance : jobInstance.getTaskInstances()){
                        taskStates.put(taskInstance.getId(),TaskState.NOT_READY);
                    }
                    jobStatus = JobStatus.READY;
                    scheduleTickMsg();
                    if (jobInstance.getTaskInstances().size() > 0)
                        curTaskInstanceId = jobInstance.getTaskInstances().get(0).getId();

                    logger.info("JobInstance:" + jobInstance.getId() + " is initialized ready to go");
                    break;

                case STATUS:


                    TaskInstance nextTaskInstance = getNextTask();
                    if (nextTaskInstance!=null){
                        logger.info("Running task " + nextTaskInstance.getId());
                        //check current task status


                    }else{
                        logger.info("No tasks pending, move on");
                    }
                    scheduleTickMsg();
                    break;


                case RUN:

                    jobStatus = JobStatus.RUNNING;
                    //schedule task one by one
                    TaskInstance taskInstance = getNextTask();
                    if (taskInstance!=null){

                        logger.info("Running task " + taskInstance.getId());

                        workerTaskRef =getWorkerTask(taskInstance);
                        if (workerTaskRef!=null){
                            workerTaskRef.tell(TaskCommand.INIT,self());
                            //TODO: wait for SUBMIT and then workout
                            workerTaskRef.tell(TaskCommand.RUN,self());
                            curTaskInstanceId = taskInstance.getId();
                            taskStates.put(curTaskInstanceId,TaskState.RUNNING);
                        }
                    }else{
                        logger.info("No tasks pending, move on");
                    }
                    reactOnTick=true;
                    logger.info("JobInstance:" + jobInstance.getId()  +" is started");
                    break;

                case CANCEL:
                    jobStatus = JobStatus.COMPLETE;
                    logger.info("JobInstance:" + jobInstance.getId() + " Cancelled");

            }

        }else if (msg instanceof TickMsg){
            logger.info("act on tick msg ");
            scheduleTickMsg();


        }else if (msg instanceof TaskResponseMsg){
            //get task id and make your own inferences





        }


    }
    //this should be FACTORY
    private ActorRef getWorkerTask(final TaskInstance taskInstance){

        if (taskInstance.getTaskType() == TaskType.COMMAND){
            return getContext().actorOf(CommandTask.create(taskInstance.getId(),
                            (CommandTaskParameters)taskInstance.getTaskParameters()),
                    "CommandTask-"+taskInstance.getId());
        }else if (taskInstance.getTaskType() == TaskType.GENIE_MR){
            return getContext().actorOf(GenieMRTask.create(taskInstance.getId(),(GenieMRTaskParameters)taskInstance.getTaskParameters()),
                    "GenieMRTask-"+ taskInstance.getId());
        }

        throw new UnsupportedOperationException("Creation of new types unsupported");
    }

    private void scheduleTickMsg(){

        getContext().system().scheduler().scheduleOnce( Duration.create(5, TimeUnit.SECONDS),
                getSelf(),JobCommand.STATUS,getContext().dispatcher(),null);

    }


    public static void main(String [] args){
        ActorSystem _system = ActorSystem.create("TaskCoordinator");

        JobInstance jobInstance = new JobInstance();
        jobInstance.setJobDefId("GenieComboRunner");

        TaskInstance genieTask =  new TaskInstance(10L,TaskType.GENIE_MR);
        final HttpServiceParams serviceParams = new HttpServiceParams("http", "localhost", 8080, "/genie/v0/jobs");
        GenieMRTaskParameters mrTaskParameters = new GenieMRTaskParameters(serviceParams, "Sumanth", "hadoop",
                "jar /tmp/hdptest.jar cruncher.TxnOperations /txndatain /txnout5",
                "file:///tmp/hdptest.jar",10,3);
        genieTask.setTaskParameters(mrTaskParameters);

        CommandTaskParameters cmdTaskParams =
                new CommandTaskParameters("python","/Users/Sumanth/scripts/tst1.py",60,2);

        TaskInstance commandTask = new TaskInstance(48L, TaskType.COMMAND);
        commandTask.setTaskParameters(cmdTaskParams);

        jobInstance.getTaskInstances().add(genieTask);
        jobInstance.getTaskInstances().add(commandTask);
        ActorRef cordRef = _system.actorOf(JobCoordinator.create(jobInstance));
        cordRef.tell(JobCommand.INIT,null);
        cordRef.tell(JobCommand.RUN,null);

    }
}
