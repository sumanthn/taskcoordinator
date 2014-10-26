package sn.execengine;

import akka.actor.Props;
import akka.actor.UntypedActor;
import sn.common.CommandTaskParameters;
import sn.task.msg.SubTaskResponse;
import sn.task.msg.TaskCommand;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sumanth on 17/10/14.
 */
public class CommandExec extends UntypedActor {
    private int commandExitVal = -1;
    private CommandTaskParameters taskParameters;

    public CommandExec(CommandTaskParameters taskParameters) {
        this.taskParameters = taskParameters;
    }

    public static Props create(final CommandTaskParameters taskParameters) {
       // return Props.create(CommandExec.class, taskParameters).withDispatcher("CommandFuturesPool");
        return Props.create(CommandExec.class,(taskParameters));
    }

    @Override
    public void onReceive(Object msg) throws Exception {

        //System.out.println("Command executor some message arrived123");
        if (msg instanceof String) {
           // System.out.println("CommandExecutor:Execute command now");
            runCommand();


            //System.out.println("Completed command run, now return");
            getContext().sender().tell(SubTaskResponse.SUCCESS, self());

        } else if (msg instanceof TaskCommand) {
            TaskCommand taskCommand = (TaskCommand) msg;

           // System.out.println("rcvd task command, respond now ");
            switch (taskCommand) {
                case STATUS:
                if (commandExitVal == 0)
                    getContext().sender().tell(SubTaskResponse.SUCCESS, self());
                else
                    getContext().sender().tell(SubTaskResponse.FAILED, self());

                    break;
            }
        }
    }

    private void runCommand() {
        String [] cmdData=null;
        if (taskParameters.getCommandParams()!=null && taskParameters.getCommandParams().length > 0) {
            cmdData = new String[taskParameters.getCommandParams().length + 1];
            cmdData[0] = taskParameters.getCommand();
            int idx=1;
            for(int i=0;i< taskParameters.getCommandParams().length;i++){
                cmdData[idx] = taskParameters.getCommandParams()[i];
                //System.out.println("input command param " + cmdData[idx]);
                idx++;
            }
        }
        else {
            cmdData = new String[1];
            cmdData[0] = taskParameters.getCommand();
        }

        /*System.out.println(ArrayUtils.toString(cmdData));
        for(int i=0;i < cmdData.length;i++){
            System.out.print(cmdData[i]);
            System.out.print(",");
        }*/
        ProcessBuilder procBuilder = new ProcessBuilder(cmdData);
        procBuilder.inheritIO();

        try {

            Process proc = procBuilder.start();
            //System.out.println("Launched command " +proc.toString());
            boolean successExec = proc.waitFor(300, TimeUnit.SECONDS);
             if (successExec) {
                int exitVal = proc.exitValue();
              //  System.out.println("Exit status as " + exitVal);
                commandExitVal = exitVal;

            } else {

               // System.out.println("Trying to kill it for now");

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

