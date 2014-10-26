package sn.execengine;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import sn.common.*;
import sn.common.def.CLITaskDef;
import sn.common.def.GenieMRTaskDef;
import sn.common.def.JobDef;
import sn.common.def.TaskDef;
import sn.task.CommandTask;
import sn.task.GenieMRTask;
import sn.task.msg.JobCommand;
import sn.task.msg.TaskCommand;
import sn.task.msg.TaskResponseMsg;
import sn.task.state.JobStatus;
import sn.task.state.TaskState;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Sumanth on 25/10/14.
 */
public class JobWorkCoordinator extends UntypedActor {

    private LoggingAdapter logger = Logging.getLogger(getContext().system(), this);


    private JobInstance jobInstance;
    private Map<Long,TaskState> taskStates ;
    private Map<Long,ActorRef> workerLeash;

    private boolean reactOnTick;

    private JobStatus jobStatus ;
    //this could be a list in case tasks are executed in parallel
   /* private long curTaskInstanceId;
    private ActorRef workerTaskRef;
   */
    private int taskNodeIdx;

    public JobWorkCoordinator(JobInstance jobInstance) {
        this.jobInstance = jobInstance;
        taskStates= new LinkedHashMap<Long,TaskState>(jobInstance.getTaskInstances().size());
        workerLeash = new LinkedHashMap<>(jobInstance.getTaskInstances().size());
        jobStatus = JobStatus.NOT_READY;
        reactOnTick=false;
        //currently tasks are serial, a simple LL would do
        taskNodeIdx = 0;

    }


    public static Props create(final JobInstance jobInstance){
        return Props.create(JobWorkCoordinator.class,jobInstance);
    }



    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg instanceof JobCommand) {
            switch ((JobCommand) msg) {


                case INIT:
                    handleJobInit();
                    break;
                case RUN:
                    handleJobRunCommand();
                    break;
                case STATUS:
                    break;
                case SUSPEND:
                    break;
                case COMPLETE:
                    break;
                case CANCEL:
                    break;
            }
        }else if (msg instanceof TaskResponseMsg) {
            TaskResponseMsg responseMsg = (TaskResponseMsg) msg;
            System.out.println("Arrived response as " + responseMsg);

            TaskState curTaskState = taskStates.get(responseMsg.getTaskId());
            switch (responseMsg.getTaskResponse()){

                case SUCCESS:
                    System.out.println("Success message came");
                    if (curTaskState == TaskState.RUNNING) {
                        logger.info("Task " + responseMsg.getTaskId() + "  completed successfully ");
                        taskStates.put(responseMsg.getTaskId(),TaskState.SUCCESS);

                    }else{
                        logger.warning("SUCCESS MSG not processed");
                    }
                    break;
                case ERROR:
                    break;
                case TIMEOUT:
                    break;
                case RUNNING:
                    System.out.println("RUNNING MESSAGE CAME");
                    if (curTaskState != TaskState.SUCCESS || curTaskState!=TaskState.FAILED) {
                        logger.info("Task " + responseMsg.getTaskId() + "  is NOW RUNNING ");
                        taskStates.put(responseMsg.getTaskId(),TaskState.RUNNING);

                    }else{
                        logger.warning("SUCCESS MSG not processed");
                    }
                    break;
                case FAILED:
                    if (curTaskState == TaskState.RUNNING) {
                        logger.info("Task " + responseMsg.getTaskId() + "  did not complete ");
                        taskStates.put(responseMsg.getTaskId(),TaskState.FAILED);

                    }else{
                        logger.warning("FAILED MSG not processed");
                    }
                    break;
            }


        }
    }

    private void handleJobInit(){
        for(TaskInstance taskInstance : jobInstance.getTaskInstances()){
            taskStates.put(taskInstance.getId(),TaskState.NOT_READY);
        }
        jobStatus = JobStatus.READY;
        //scheduleTickMsg();

        logger.info("JobInstance:" + jobInstance.getId() + " is initialized ready to go");
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


    private void runNextTask(){
        TaskInstance nextTaskInstance = getNextTask();
        if (nextTaskInstance!=null) {
            logger.info("Running task " +  nextTaskInstance.getId() + " " + nextTaskInstance.getTaskType());
            ActorRef workerTaskRef = getWorkerTask(nextTaskInstance);
            //TODO: handle msg from INIT TASK
            workerTaskRef.tell(TaskCommand.INIT, self());
            //TODO: wait for SUBMIT and then workout
            workerTaskRef.tell(TaskCommand.RUN, self());

            taskStates.put(nextTaskInstance.getId(), TaskState.SUBMITTED);
            workerLeash.put(nextTaskInstance.getId(),workerTaskRef);
        }else{
            logger.info("No more tasks to run");
            getContext().self().tell( JobCommand.COMPLETE,self());
            getContext().stop(self());
        }

    }
    private ActorRef getWorkerTask(final TaskInstance taskInstance){

        if (taskInstance.getTaskType() == TaskType.COMMAND){
            return getContext().actorOf(sn.execengine.CommandTask.create(taskInstance.getId(),
                            (CommandTaskParameters) taskInstance.getTaskParameters()),
                    "CommandTask-"+taskInstance.getId());
        }else if (taskInstance.getTaskType() == TaskType.GENIE_MR){
            return getContext().actorOf(GenieMRTask.create(taskInstance.getId(), (GenieMRTaskParameters) taskInstance.getTaskParameters()),
                    "GenieMRTask-"+ taskInstance.getId());
        }

        throw new UnsupportedOperationException("Creation of new types unsupported");
    }

    private void handleJobRunCommand(){

        jobStatus = JobStatus.RUNNING;

        runNextTask();
        reactOnTick=true;
        logger.info("JobInstance:" + jobInstance.getId()  +" is started");

    }
    public static JobInstance getBytesAggTask(){
        JobDef jobDef = new JobDef("7724dd65-dc8e-456b-8d27-4c718da9013d","URLResponseAggregator","AnalyticsPipe");
        jobDef.setDescription("Aggregate URL response");
        List<TaskDef> taskDefs = new ArrayList<TaskDef>();

        String [] cmdParams =  new String[]{
                "/Users/Sumanth/scripts/tst1.py"
        };

        TaskDef cleanupTask = new CLITaskDef("27946295-f1ca-44d5-96be-d4984fc1035b","DirCleanTask",
                "python",cmdParams);
        taskDefs.add(cleanupTask);

        final HttpServiceParams serviceParams = new HttpServiceParams("http", "localhost", 8080, "/genie/v0/jobs");
        GenieMRTaskDef serverIpAggTask = new GenieMRTaskDef("ad3fb1a3-1b83-4ac7-a299-32b6894d2319","URLResponseAggTask",serviceParams,
                "Sumanth","hadoop");
        serverIpAggTask.setTimeout(3600);
        serverIpAggTask.setHadoopCommand("jar /tmp/hdptest.jar cruncher.BytesAggregatorRunner /txndatain /bytesagg");
        serverIpAggTask.setHadoopFiles("file:///tmp/hdptest.jar");

       // taskDefs.add(serverIpAggTask);

        String [] stageTaskParams =  new String[]{
                "/Users/Sumanth/scripts/tst1.py"
        };

        TaskDef stageResTask = new CLITaskDef("2d98903a-842f-44af-bff5-d0d7de3924c0","StageURLResTask",
                "python",stageTaskParams);


        taskDefs.add(stageResTask);

        jobDef.setTaskDefinition(taskDefs);

        return jobDef.buildJobInstance();
    }

    public static void main(String [] args){
        ActorSystem _system = ActorSystem.create("TaskCoordinator");


        JobInstance bytesAggInstance = getBytesAggTask();

        //ActorRef cordRef = _system.actorOf(JobCoordinator.create(jobInstance),"JobCoordinator-"+jobInstance.getId());
        ActorRef cordRef = _system.actorOf(JobWorkCoordinator.create(bytesAggInstance),"JobCoordinator-"+bytesAggInstance.getId());
        cordRef.tell(JobCommand.INIT,null);
        cordRef.tell(JobCommand.RUN,null);

    }
}
