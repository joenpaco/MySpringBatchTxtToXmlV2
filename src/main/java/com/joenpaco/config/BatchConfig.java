package com.joenpaco.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import com.joenpaco.models.ExamResult;
import com.joenpaco.springbatch.ExamResultFieldSetMapper;
import com.joenpaco.springbatch.ExamResultItemProcessor;
import com.joenpaco.springbatch.ExamResultJobListener;

@Configuration
@PropertySource(ResourceNames.PROPERTIES)
@ComponentScan(basePackages = { ResourceNames.PACKAGE })
@EnableBatchProcessing
public class BatchConfig {

	@Autowired
	private Environment environment;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	final static String Step1 = "step1";
	final static Integer size = 10;
	

	@Bean
	public JobRepository jobRepository() throws Exception {
		return new MapJobRepositoryFactoryBean().getObject();
	}

	@Bean
	public SimpleJobLauncher jobLauncher() throws Exception {
		SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
		simpleJobLauncher.setJobRepository(jobRepository());

		return simpleJobLauncher;
	}

	// Reader
	@Bean
	public FlatFileItemReader<ExamResult> flatFileItemReader() {

		Resource resource = new FileSystemResource(environment.getProperty("file.origin.name"));
		FlatFileItemReader<ExamResult> flatFileItemReader = new FlatFileItemReader<ExamResult>();
		flatFileItemReader.setResource(resource);
		flatFileItemReader.setLineMapper(lineMapper());
		return flatFileItemReader;
	}

	@Bean
	public DelimitedLineTokenizer lineTokenizer() {
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setDelimiter("|");
		return lineTokenizer;
	}
	
    @Bean
    public ExamResultFieldSetMapper fieldSetMapper()
    {
    	ExamResultFieldSetMapper fieldSetMapper = new ExamResultFieldSetMapper();
    	
    	return fieldSetMapper;
    }

	@Bean
	public DefaultLineMapper<ExamResult> lineMapper() {
		DefaultLineMapper<ExamResult> lineMapper = new DefaultLineMapper<ExamResult>();
		lineMapper.setFieldSetMapper(fieldSetMapper());
		lineMapper.setLineTokenizer(lineTokenizer());
		return lineMapper;
	}
	
    //Writer
	@Bean
	public StaxEventItemWriter<ExamResult> writer() {
		StaxEventItemWriter<ExamResult> writer = new StaxEventItemWriter<ExamResult>();
		Resource resource = new FileSystemResource(environment.getProperty("file.target.name"));
		String rootTagName = "UniversityExamResultList";
		Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
		marshaller.setClassesToBeBound(ExamResult.class);
		writer.setResource(resource);
		writer.setRootTagName(rootTagName);
		writer.setMarshaller(marshaller);
		return writer;
	}
	
	@Bean
	public ExamResultItemProcessor processor() {
		return new ExamResultItemProcessor();
	}

	@Bean
	public ExamResultJobListener jobListener() {
		return new ExamResultJobListener();
	}
	
	@Bean
	public ResourcelessTransactionManager transactionManager() {
		return new ResourcelessTransactionManager();
	}
	
	@Bean
	public Job job() {
		return jobBuilderFactory.get("job").incrementer(new RunIdIncrementer()).listener(jobListener()).flow(step1())
				.end().build();
	}
	
	@Bean
	public Step step1() {
		return stepBuilderFactory.get(Step1).<ExamResult, ExamResult>chunk(size).reader(flatFileItemReader())
				.processor(processor()).writer(writer()).build();
	}

		
}
