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
 * There can be many Job co-ordinator
 * Simple sequential
 * DAG based
 * Multi-parallel co-ordinator
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


                   if (workerTaskRef!=null){
                       workerTaskRef.tell(TaskCommand.STATUS,self());
                   }
                    scheduleTickMsg();
                    break;


                case RUN:

                    jobStatus = JobStatus.RUNNING;

                    runNextTask();
                    reactOnTick=true;
                    logger.info("JobInstance:" + jobInstance.getId()  +" is started");
                    break;

                case CANCEL:
                    jobStatus = JobStatus.COMPLETE;
                    logger.info("JobInstance:" + jobInstance.getId() + " Cancelled");

                    break;
                case COMPLETE:
                    jobStatus = JobStatus.COMPLETE;
                    for(Long taskId : taskStates.keySet()){

                        logger.info("Task " + taskId + " State " + taskStates.get(taskId));
                    }
                    logger.info("Tasks are completed, run through post completion ");
                    for(ActorRef actorRef : getContext().getChildren()){
                        actorRef.tell(TaskCommand.DESTROY,self());
                    }
                    getContext().stop(self());
                    break;
            }

        }else if (msg instanceof TickMsg){
            logger.info("act on tick msg ");
            scheduleTickMsg();


        }else {
            if (msg instanceof TaskResponseMsg) {
                //get task id and make your own inferences


                TaskResponseMsg responseMsg = (TaskResponseMsg) msg;
                System.out.println("Arrived response as " + responseMsg);

                switch (responseMsg.getTaskResponse()) {


                    case SUCCESS:
                        taskStates.put(responseMsg.getTaskId(),responseMsg.getTaskState());
                        if (responseMsg.getTaskState() == TaskState.SUCCESS){
                            //run next task, based on response
                            logger.info(responseMsg.getTaskId() + " is completed");
                            runNextTask();
                        }

                        break;


                    case TIMEOUT:
                        break;
                    case RUNNING:
                        taskStates.put(responseMsg.getTaskId(),responseMsg.getTaskState());
                        logger.info(responseMsg.getTaskId() + " is still running");
                        break;
                    case ERROR:

                    case FAILED:
                        taskStates.put(responseMsg.getTaskId(),responseMsg.getTaskState());
                        if (responseMsg.getTaskState() == TaskState.FAILED){
                            logger.info("Failed task " + responseMsg.getTaskId());
                            getContext().self().tell( JobCommand.COMPLETE,self());
                        }

                        break;
                }


            }
        }


    }
    private void runNextTask(){
        TaskInstance nextTaskInstance = getNextTask();
        if (nextTaskInstance!=null) {
            logger.info("Running task " +  nextTaskInstance.getId() + " " + nextTaskInstance.getTaskType());
            workerTaskRef = getWorkerTask(nextTaskInstance);
            workerTaskRef.tell(TaskCommand.INIT, self());
            //TODO: wait for SUBMIT and then workout
            workerTaskRef.tell(TaskCommand.RUN, self());
            curTaskInstanceId = nextTaskInstance.getId();
            taskStates.put(curTaskInstanceId, TaskState.RUNNING);
        }else{
            logger.info("No more tasks to run");
            getContext().self().tell( JobCommand.COMPLETE,self());
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

        CommandTaskParameters cleanDirTaskParams =
                new CommandTaskParameters("python","/Users/Sumanth/scripts/cleandir.py",60,2);

        TaskInstance cleanupTask = new TaskInstance(4L, TaskType.COMMAND);
        cleanupTask.setTaskParameters(cleanDirTaskParams);

        TaskInstance genieTask =  new TaskInstance(9L,TaskType.GENIE_MR);
        final HttpServiceParams serviceParams = new HttpServiceParams("http", "localhost", 8080, "/genie/v0/jobs");
        GenieMRTaskParameters mrTaskParameters = new GenieMRTaskParameters(serviceParams, "Sumanth", "hadoop",
                "jar /tmp/hdptest.jar cruncher.TxnOperations /txndatain /faultyserveripdataout",
                "file:///tmp/hdptest.jar",120*10,3);
        genieTask.setTaskParameters(mrTaskParameters);

        CommandTaskParameters stageResultsTaskParams =
                new CommandTaskParameters("python","/Users/Sumanth/scripts/stageresults.py",60,2);

        TaskInstance stageResultTask = new TaskInstance(25L, TaskType.COMMAND);
        stageResultTask.setTaskParameters(stageResultsTaskParams);

        jobInstance.getTaskInstances().add(cleanupTask);
        jobInstance.getTaskInstances().add(genieTask);
        jobInstance.getTaskInstances().add(stageResultTask);

        ActorRef cordRef = _system.actorOf(JobCoordinator.create(jobInstance),"JobCoordinator-"+jobInstance.getId());
        cordRef.tell(JobCommand.INIT,null);
        cordRef.tell(JobCommand.RUN,null);

    }
}
