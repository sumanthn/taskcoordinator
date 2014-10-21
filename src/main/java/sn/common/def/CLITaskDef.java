package sn.common.def;

import jdk.nashorn.internal.objects.Global;
import sn.common.CommandTaskParameters;
import sn.common.GlobalIdAssigner;
import sn.common.TaskInstance;
import sn.common.TaskType;

/**
 * Created by Sumanth on 20/10/14.
 */
public class CLITaskDef extends TaskDef {
    private final String command;
    private final String commandParams;

    public CLITaskDef(String uuid, String command, String commandParams) {
        super(uuid);
        this.command = command;
        this.commandParams = commandParams;
    }

    public CLITaskDef(String uuid, String name, String command, String commandParams) {
        super(uuid, name);
        this.command = command;
        this.commandParams = commandParams;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CLITaskDef that = (CLITaskDef) o;

        if (!uuid.equals(that.uuid)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }

    @Override
    public synchronized TaskInstance buildTaskInstance() {

        TaskInstance taskInstance = new TaskInstance(GlobalIdAssigner.getInstance().getId(),TaskType.COMMAND);
        taskInstance.setTaskUuid(getUuid());
        CommandTaskParameters cmdTaskParams =
                new CommandTaskParameters(command,commandParams,
                        timeout,retry);

        taskInstance.setTaskParameters(cmdTaskParams);
        return taskInstance;

        /*
        CommandTaskParameters stageResultsTaskParams =
                new CommandTaskParameters("python","/Users/Sumanth/scripts/stageresults.py",60,2);

        TaskInstance stageResultTask = new TaskInstance(25L, TaskType.COMMAND);
        stageResultTask.setTaskParameters(stageResultsTaskParams);*/

    }
}
