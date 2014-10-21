package sn.jobmanager;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import sn.common.def.JobDef;

/**
 * Created by Sumanth on 21/10/14.
 */
public class JobSubmitter implements Job{


    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        System.out.println("Submit JOB to JobQueue");
        JobDef jobDef = (JobDef) jobExecutionContext.getJobDetail().getJobDataMap().get("Jobdef");
        JobManager.getInstance().launchJob(jobDef);

    }
}
