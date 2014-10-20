package sn.common;

import sn.common.TaskParameters;

/**
 * Created by Sumanth on 17/10/14.
 */
public class CommandTaskParameters extends TaskParameters {
    private final String command;
    private final String commandParams;


    public CommandTaskParameters(final String command, final String params, int timeout, int retry) {
        super(timeout, retry);
        this.command = command;
        this.commandParams = params;
    }


    public String getCommand() {
        return command;
    }

    public String getCommandParams() {
        return commandParams;
    }
}
