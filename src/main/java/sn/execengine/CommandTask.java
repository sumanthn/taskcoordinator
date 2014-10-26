package sn.execengine;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.typesafe.config.Config;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import sn.common.CommandTaskParameters;
import sn.common.GlobalIdAssigner;
import sn.task.CommandExecutor;
import sn.task.RunnableTask;
import sn.task.msg.JobCommand;
import sn.task.msg.SubTaskResponse;
import sn.task.msg.TaskCommand;
import sn.task.msg.TaskResponse;
import sn.task.state.TaskState;

import java.util.concurrent.TimeUnit;

/**
 * Created by Sumanth on 17/10/14.
 */
public class CommandTask extends  RunnableTask{
    LoggingAdapter logger = Logging.getLogger(getContext().system(), this);
   private Future<Object> promise;
   private ActorRef commandExecutor;
   private CommandTaskParameters taskParameters;

   //private TaskState curState;

    public CommandTask(final long taskId, CommandTaskParameters taskParameters) {
        this.taskId = taskId;
        curState = TaskState.READY;
        this.taskParameters = taskParameters;
    }

    public static Props create(final long taskId,final CommandTaskParameters taskParameters){
        //return Props.create(CommandTask.class,taskId,taskParameters).withDispatcher("CommandFuturesPool");
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
                        commandExecutor = getContext().actorOf(CommandExec.create(taskParameters));

                        logger.info("Commandtask has submitted and waiting123");
                        promise = Patterns.ask(commandExecutor, "Execute",
                                Timeout.apply(taskParameters.getTimeout(), TimeUnit.SECONDS));

                        curState = TaskState.RUNNING;
                        //notifyParent(TaskResponse.SUCCESS);
                        //getContext().sender().tell(TaskResponseMsg.SUCCESS, self());
                        parent = sender();
                        System.out.println("Setting up parent " + parent.toString());
                        notifyParent(TaskResponse.RUNNING);
                        pingSelf();
                    }
                    break;
                case STATUS:
                    //check status and report back
                    if (curState == TaskState.RUNNING ) {
                        logger.debug("CommandTask:Check status for ");
                        //check for URL status and if completed say SUCCESS
                        boolean hasActorCompleted = promise.isCompleted();
                       // System.out.println("Actor Action Complete is " + hasActorCompleted);
                        if (hasActorCompleted == true) {
                            logger.info("Command Execution is complete, check for status");
                            //getContext().sender().tell(TaskResponseMsg.RUNNING,self());
                           //cannot afford another blocking call
                            curState = TaskState.AWAIT_RESPONSE;
                          commandExecutor.tell(TaskCommand.STATUS,self());

                        } else {
                       /* logger.info("Command execution in progress");
                        curState = TaskState.RUNNING;
                        notifyParent(TaskResponse.RUNNING);
                        */

                        }

                        pingSelf();
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
        }else if (msg instanceof SubTaskResponse){

            if (curState == TaskState.AWAIT_RESPONSE){
                SubTaskResponse subTaskResponse =  (SubTaskResponse) msg;
                if (subTaskResponse == SubTaskResponse.SUCCESS){
                    logger.info("Subtask completed successfully");
                    curState = TaskState.SUCCESS;
                    notifyParent(TaskResponse.SUCCESS);

                }else{
                    curState = TaskState.FAILED;
                    logger.info("Subtask failed to execute");
                    notifyParent(TaskResponse.FAILED);
                }

            }
        }
    }


    public static void main(String [] args){


        ActorSystem _system = ActorSystem.create("CommandTask");


        String [] cmdParams = new String[]{
                "/Users/Sumanth/scripts/tst1.py"
        };

        CommandTaskParameters cmdTaskParams =
                new CommandTaskParameters("python",cmdParams,60,2);
        int count =0;
        for(int i=0;i < 1000;i++) {
            ActorRef cordRef = _system.actorOf(CommandTask.create(GlobalIdAssigner.getInstance().getId()
                    , cmdTaskParams));
            cordRef.tell(TaskCommand.SUBMIT, null);

            count++;
            if (count % 1000==0)
                System.out.println("Done submitting " + count);

            /*try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
        }

        System.out.println("Done submitting " + count);

        /*for(int i =0;i < 5;i++) {
            cordRef.tell(TaskCommand.STATUS, null);
            try {
                Thread.sleep(20*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
*/
    }



}
