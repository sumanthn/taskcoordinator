package sn.jobmanager;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import sn.common.GlobalIdAssigner;
import sn.common.def.JobDef;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.quartz.JobBuilder.newJob;

/**
 * Created by Sumanth on 21/10/14.
 */
public class ScheduleManager {

    private static ScheduleManager ourInstance = new ScheduleManager();

    private Scheduler scheduler ;
    public static ScheduleManager getInstance() {
        return ourInstance;
    }

    private ScheduleManager() {
    }
    public void init(){
        try {
            scheduler = new StdSchedulerFactory().getScheduler();
            scheduler.start();
        } catch (SchedulerException e) {
            e.printStackTrace();
        }

    }

    public void loadJobInstance(final JobDef jobDef, final String cronExpression){

        //make it into an instance
        //each instance has a trigger

        //use http://www.cronmaker.com/ to makeup cron expressions
        Map<String,JobDef> jobDefMap = new HashMap<String, JobDef>();
        jobDefMap.put("Jobdef",jobDef);
        JobDataMap jobDataMap = new JobDataMap(jobDefMap);

        //check if exists, if not then work it out
        System.out.println("project name " + jobDef.getProjectName() + " name is " + jobDef.getName());
        JobDetail job = newJob(JobSubmitter.class)
                .withIdentity(jobDef.getName(), jobDef.getProjectName()).setJobData(jobDataMap)
                .build();

        CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(UUID.randomUUID().toString(),jobDef.getProjectName())
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
                .build();

        try {
            scheduler.scheduleJob(job,trigger);
        } catch (SchedulerException e) {
            e.printStackTrace();
        }


    }
}
