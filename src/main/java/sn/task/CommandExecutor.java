package sn.task;

import akka.actor.Props;
import akka.actor.UntypedActor;
import sn.common.CommandTaskParameters;
import sn.task.msg.SubTaskResponse;
import sn.task.msg.TaskCommand;
import sn.task.msg.TaskResponseMsg;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sumanth on 17/10/14.
 */
public class CommandExecutor extends UntypedActor {
    private int commandExitVal = -1;
    private CommandTaskParameters taskParameters;

    public CommandExecutor(CommandTaskParameters taskParameters) {
        this.taskParameters = taskParameters;
    }

    public static Props create(final CommandTaskParameters taskParameters) {
        return Props.create(CommandExecutor.class, taskParameters);
    }

    @Override
    public void onReceive(Object msg) throws Exception {

        System.out.println("Command executor some message arrived");
        if (msg instanceof String) {
            System.out.println("CommandExecutor:Execute command now");
            runCommand();


            System.out.println("Completed command run, now return");
            getContext().sender().tell("Completed", self());

        } else if (msg instanceof TaskCommand) {
            System.out.println("rcvd task command, respond now ");
            if (commandExitVal == 0)
                getContext().sender().tell(SubTaskResponse.SUCCESS,self());
            else
                getContext().sender().tell(SubTaskResponse.FAILED,self());
        }
    }

    private void runCommand() {
        ProcessBuilder procBuilder = new ProcessBuilder(taskParameters.getCommand(), taskParameters.getCommandParams());
        procBuilder.inheritIO();

        try {

            Process proc = procBuilder.start();
            System.out.println("Launched command");
            boolean successExec = proc.waitFor(300, TimeUnit.SECONDS);
            System.out.println(successExec);
            if (successExec) {
                int exitVal = proc.exitValue();
                System.out.println("Exit status as " + exitVal);
                commandExitVal = exitVal;

            } else {

                System.out.println("Trying to kill it for now");

                proc.destroyForcibly();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();

        } catch (IllegalThreadStateException e2) {
            e2.printStackTrace();

        }
    }
}

