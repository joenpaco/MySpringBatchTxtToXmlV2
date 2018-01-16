package com.joenpaco.springbatch;

import org.springframework.batch.item.ItemProcessor;

import com.joenpaco.models.ExamResult;

public class ExamResultItemProcessor implements ItemProcessor<ExamResult, ExamResult> {


	    public ExamResult process(ExamResult result) throws Exception {
	        System.out.println("Processing result :"+result);
	        if(result.getPercentage() < 60){
	            return null;
	        }
	 
	        return result;
	    }
}
