package sn.task;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Future;
import sn.common.CommandTaskParameters;
import sn.task.msg.SubTaskResponse;
import sn.task.msg.TaskCommand;
import sn.task.msg.TaskResponse;
import sn.task.msg.TaskResponseMsg;
import sn.task.state.TaskState;

import java.util.concurrent.TimeUnit;

/**
 * Created by Sumanth on 17/10/14.
 */
public class CommandTask  extends  RunnableTask{
    LoggingAdapter logger = Logging.getLogger(getContext().system(), this);
   private Future<Object> promise;
   private ActorRef commandExecutor;
   private CommandTaskParameters taskParameters;

   //private TaskState curState;

    public CommandTask(final long taskId,CommandTaskParameters taskParameters) {
        this.taskId = taskId;
        curState = TaskState.READY;
        this.taskParameters = taskParameters;
    }

    public static Props create(final long taskId,final CommandTaskParameters taskParameters){
        return Props.create(CommandTask.class,taskId,taskParameters);
    }

    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg instanceof TaskCommand){


            switch((TaskCommand)msg){

                case INIT:
                    //make it ready
                    curState = TaskState.READY;
                    break;
                case RUN:


                case SUBMIT:

                    //submit for execution
                    //check if success
                    if (curState ==TaskState.READY) {
                        commandExecutor = getContext().actorOf(CommandExecutor.create(taskParameters));

                        logger.info("Commandtask has submitted and waiting");
                        promise = Patterns.ask(commandExecutor, "Execute",
                                Timeout.apply(taskParameters.getTimeout(), TimeUnit.SECONDS));

                        curState = TaskState.RUNNING;
                        notifyParent(TaskResponse.RUNNING);
                        System.out.println("Scheduled command in future");
                        //getContext().sender().tell(TaskResponseMsg.SUCCESS, self());

                    }
                    break;
                case STATUS:
                    //check status and report back

                    logger.debug("CommandTask:Check status for " );
                    //check for URL status and if completed say SUCCESS
                    boolean hasActorCompleted = promise.isCompleted();
                    System.out.println("Actor Action Complete is " + hasActorCompleted);
                    if (hasActorCompleted== true){
                        logger.info("Command Execution is complete, check for status");
                        //getContext().sender().tell(TaskResponseMsg.RUNNING,self());
                        Future<Object> taskCompleteStatus=
                        Patterns.ask(commandExecutor, TaskCommand.STATUS,
                                Timeout.apply(taskParameters.getTimeout(), TimeUnit.SECONDS));


                        //this is very fast so block
                        Thread.sleep(20*20);
                        //TODO: change this sleep to onComplete
                        if  ( (SubTaskResponse)(taskCompleteStatus.value().get().get())  == SubTaskResponse.SUCCESS){
                            logger.info("Command executed successfully");
                            curState = TaskState.SUCCESS;
                            notifyParent(TaskResponse.SUCCESS);

                        }else{
                            logger.info("Command failed in execution");
                           //move to base class
                            curState=TaskState.FAILED;
                            notifyParent(TaskResponse.FAILED);

                        }




                     }else{
                        logger.info("Command execution in progress");
                        curState = TaskState.RUNNING;
                        notifyParent(TaskResponse.RUNNING);

                    }


                    break;
                case CANCEL:
                    //invoke cancel URL
                    System.out.println("invoking cancel on task");
                    break;
                case DESTROY:
                    System.out.println("Invoking DESTTORY");

                    getContext().stop(self());
                    break;
            }
        }else if (msg instanceof String){
            System.out.println("Rcvd message from command worker");
        }
    }


    public static void main(String [] args){
        ActorSystem _system = ActorSystem.create("CommandTask");

        CommandTaskParameters taskParams =  new CommandTaskParameters("python","/Users/Sumanth/scripts/tst1.py",60,2);
        ActorRef cordRef = _system.actorOf(CommandTask.create(15L,taskParams));
        cordRef.tell(TaskCommand.SUBMIT,null);

        try {
            Thread.sleep(5*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for(int i =0;i < 5;i++) {
            cordRef.tell(TaskCommand.STATUS, null);
            try {
                Thread.sleep(20*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }



}
