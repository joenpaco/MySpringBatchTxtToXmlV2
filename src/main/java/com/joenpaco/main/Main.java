package com.joenpaco.main;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.joenpaco.config.BatchConfig;
 
public class Main {
	
	private static Logger logger = LogManager.getLogger();
	
    @SuppressWarnings("resource")
    public static void main(String args[]){
 
    	AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(BatchConfig.class);
 
        JobLauncher jobLauncher = (JobLauncher) context.getBean("jobLauncher");
        Job job = (Job) context.getBean("job");
 
        try {
            JobExecution execution = jobLauncher.run(job, new JobParameters());
            logger.info("Job Exit Status : "+ execution.getStatus());
 
        } catch (JobExecutionException e) {
            logger.info("Job ExamResult failed");
            e.printStackTrace();
        }
    }
 
}