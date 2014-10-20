package sn.common;

/**
 * Created by Sumanth on 17/10/14.
 */
public class GenieMRTaskParameters extends TaskParameters {

    private final HttpServiceParams httpServiceParams;
    private final String userName;
    private final String groupName;

    private String hadoopCommand;
    private String hadoopFiles;


    public GenieMRTaskParameters(HttpServiceParams httpServiceParams, final String userName, final String groupName,
                                 String hadoopCommand, String hadoopFiles, final int taskTimeout, final int retries) {
        super(taskTimeout,retries);
        this.httpServiceParams = httpServiceParams;
        this.userName = userName;
        this.groupName =  groupName;

        this.hadoopCommand = hadoopCommand;
        this.hadoopFiles = hadoopFiles;
    }

    public String getUserName() {
        return userName;
    }

    public String getGroupName() {
        return groupName;
    }

    public HttpServiceParams getHttpServiceParams() {
        return httpServiceParams;
    }

    public String getHadoopCommand() {
        return hadoopCommand;
    }

    public void setHadoopCommand(String hadoopCommand) {
        this.hadoopCommand = hadoopCommand;
    }

    public String getHadoopFiles() {
        return hadoopFiles;
    }

    public void setHadoopFiles(String hadoopFiles) {
        this.hadoopFiles = hadoopFiles;
    }
}
