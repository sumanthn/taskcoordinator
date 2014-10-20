package sn.task;

import akka.actor.UntypedActor;
import sn.task.msg.TaskResponse;
import sn.task.msg.TaskResponseMsg;
import sn.task.state.TaskState;

/**
 * Created by Sumanth on 17/10/14.
 */
public abstract class RunnableTask extends UntypedActor{

    //though it is sen
    protected TaskState curState;
    protected long taskId;

    public void notifyParent(final TaskResponse taskResponse){
        getContext().sender().tell(new TaskResponseMsg(taskId,curState,taskResponse),self());

    }

    public void notifyParent(final TaskResponse taskResponse,final String msg){
        getContext().sender().tell(new TaskResponseMsg(taskId,curState,taskResponse,msg),self());
    }

    protected void setCurrentState(final TaskState taskState){


    }



}
