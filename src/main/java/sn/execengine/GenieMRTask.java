package sn.execengine;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import sn.common.GenieMRTaskParameters;
import sn.common.HttpOperation;
import sn.common.HttpServiceParams;
import sn.task.RunnableTask;
import sn.task.msg.TaskCommand;
import sn.task.msg.TaskResponse;
import sn.task.state.TaskState;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Random;

/**
 * Created by Sumanth on 17/10/14.
 */
public class GenieMRTask extends RunnableTask {
    private LoggingAdapter logger = Logging.getLogger(getContext().system(), this);

    private final GenieMRTaskParameters taskParameters;
    private static final Random random = new Random();
    String genieJobId;
    String statusUrl;
    String killUrl;
    static final Gson gson = new Gson();

    public GenieMRTask(final long taskId, final GenieMRTaskParameters taskParameters) {
        this.taskId = taskId;
        this.taskParameters = taskParameters;

    }

    public static Props create(final long taskId,final GenieMRTaskParameters taskParameters){
        return Props.create(GenieMRTask.class,taskId,taskParameters);
    }



    private class Payload {
        String jobName = "HadoopTask" + random.nextInt();
        String userName = "Sumanth";
        String groupName = "hadoop";
        String userAgent = "laptop";
        String jobType = "hadoop";
        String schedule = "adHoc";
        String cmdArgs = "jar /tmp/hadoop-examples.jar wordcount /input /out1";
        String fileDependencies = "file:///tmp//hadoop-examples.jar";

        public Payload() {
            userName = taskParameters.getUserName();
            groupName = taskParameters.getGroupName();
            cmdArgs = taskParameters.getHadoopCommand();
            fileDependencies = taskParameters.getHadoopFiles();
        }


    }

    private class GenieTaskData {
        Payload jobInfo;

        public GenieTaskData(final Payload jobInfo) {
            this.jobInfo = jobInfo;
        }



    }

    @Override
    public void onReceive(Object msg) throws Exception {

        if (msg instanceof TaskCommand){

            switch((TaskCommand)(msg)){

                case INIT:
                    if(taskParameters!=null)
                        curState = TaskState.READY;
                    break;
                case RUN:

                case SUBMIT:
                    TaskState state = submitJob();
                    if(state==TaskState.SUCCESS){
                       // getContext().sender().tell(TaskResponseMsg.SUCCESS,self());
                        curState = TaskState.RUNNING;
                        notifyParent(TaskResponse.SUCCESS);
                        logger.info("MR task " +  genieJobId + " submitted successfully");
                    }

                    break;
                case STATUS:

                    TaskState curState = checkStatus();
                    if (curState==TaskState.FAILED){
                        this.curState = TaskState.FAILED;
                        notifyParent(TaskResponse.FAILED);
                        logger.info("MR task " + genieJobId + " Failed to complete");

                        //getContext().sender().tell(TaskResponseMsg.FAILED,self());
                    }else if(curState ==TaskState.SUCCESS){
                        //getContext().sender().tell(TaskResponseMsg.SUCCESS,self());
                        this.curState = TaskState.SUCCESS;
                        notifyParent(TaskResponse.SUCCESS);
                        logger.info("MR task " + genieJobId + " successfully completed");
                    }else{
                        //getContext().sender().tell(TaskResponseMsg.RUNNING,self());
                        this.curState = TaskState.RUNNING;

                        notifyParent(TaskResponse.RUNNING);
                        logger.info("MR task " + genieJobId + " still running");
                    }
                    break;
                case CANCEL:
                    //TODO: invoke the kill URI
                    //this is not tested
                    break;
                case DESTROY:
                    getContext().stop(self());
                    break;
            }
        }


    }

    public String buildPayload() {
        GenieTaskData taskData = new GenieTaskData(new Payload());
        logger.debug("Payload JSON:" + gson.toJson(taskData));
        return gson.toJson(taskData);

    }

    private TaskState submitJob() {
        //build jobs & execute the request

        try {
            HttpResponse response =
                    executeHttpRequest(taskParameters.getHttpServiceParams().buildUri(), HttpOperation.POST,
                            new StringEntity(buildPayload()));
            //parse the JSON response and set the URIs and task states

            if (response == null) return TaskState.FAILED;

            if (!(response.getStatusLine().getStatusCode() >= 200 && response.getStatusLine().getStatusCode() < 300)) {
                return TaskState.FAILED;
            }
            String temp = EntityUtils.toString(response.getEntity());

            logger.debug("response was" + temp);

            JsonElement jelem = gson.fromJson(temp, JsonElement.class);
            JsonObject jobj = jelem.getAsJsonObject();

            JsonObject jobInfoResponse = jobj.get("jobs").getAsJsonObject().get("jobInfo").getAsJsonObject();
             killUrl = jobInfoResponse.get("killURI").getAsString();


            String status = jobInfoResponse.get("status").getAsString();
             genieJobId = jobInfoResponse.get("jobID").getAsString();
            statusUrl = taskParameters.getHttpServiceParams().buildUri() + "/" + genieJobId;



            logger.info("Hadoop MR job launched:" + genieJobId + " Status:" +  status);


            return TaskState.SUCCESS;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.warning("Failed in launching Hadoop MR task");
        return TaskState.FAILED;

    }

    private TaskState checkStatus() {
        HttpResponse response = executeHttpRequest(statusUrl, HttpOperation.GET, null);

        if (response == null) return TaskState.FAILED;

        String responseStr = null;
        try {
            responseStr = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.debug("response was" + responseStr);
        JsonElement jelem = gson.fromJson(responseStr, JsonElement.class);
        JsonObject jobj = jelem.getAsJsonObject();
        //System.out.println(jobj.get("jobs"));

        JsonObject jobInfoResponse = jobj.get("jobs").getAsJsonObject().get("jobInfo").getAsJsonObject();

        String status = jobInfoResponse.get("status").getAsString();

        if (status.equals("RUNNING")) {
            logger.info("job id " + genieJobId + "is running");
            return TaskState.RUNNING;
        } else if (status.equals("FAILED")) {
            logger.info("Job " + genieJobId + " is failed");

            return TaskState.FAILED;

        } else if (status.equals("SUCCEEDED")) {
            logger.info("Job " + genieJobId + " is Successfully completed");
            return TaskState.SUCCESS;

        } else {
            logger.warning("Invalid Status String" + status + " for HadoopMR Task " + genieJobId);
            return TaskState.RUNNING;
        }
    }


    private HttpResponse executeHttpRequest(final String url, final HttpOperation httpOperation,
                                            final StringEntity entity) {

        //all interactions occur over Json
        CloseableHttpClient httpclient = HttpClients.createDefault();

        try {

            if (httpOperation == HttpOperation.POST) {
                HttpPost httppost = new HttpPost(url);

                System.out.println("Using URL " + url);
                //TODO: set time out for HTTP operation
                //se.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE, "application/json"));
                httppost.setHeader("Content-type", "application/json");
                httppost.setHeader("Accept", "application/json");
                httppost.setEntity(entity);

                return httpclient.execute(httppost);
            } else if (httpOperation == HttpOperation.GET) {
                HttpGet httpGet = new HttpGet(url);

                httpGet.setHeader("Content-type", "application/json");


                httpGet.setHeader("Accept", "application/json");
                return httpclient.execute(httpGet);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }


    public static Props GenieMRTask(final GenieMRTaskParameters taskParameters) {
        return Props.create(GenieMRTask.class, taskParameters);
    }


    public static void main(String[] args) {

        final HttpServiceParams serviceParams = new HttpServiceParams("http", "localhost", 8080, "/genie/v0/jobs");
        GenieMRTaskParameters mrTaskParameters = new GenieMRTaskParameters(serviceParams, "Sumanth", "hadoop",
                "jar /tmp/hdptest.jar cruncher.TxnOperations /txndatain /txnout5",
                "file:///tmp/hdptest.jar",1200,3);
/*

        GenieMRTask mrTask = new GenieMRTask(mrTaskParameters);
        mrTask.buildPayload();



        mrTask.submitJob();

        for (int i = 0; i < 20; i++) {
            try {
                Thread.sleep(2 * 1000);
                mrTask.checkStatus();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
*/

        ActorSystem _system = ActorSystem.create("GenieTask");
        ActorRef cordRef = _system.actorOf(GenieMRTask.create(10L, mrTaskParameters));
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
