package com.example.batchpartitionbug;

import org.jspecify.annotations.Nullable;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.support.JdbcDefaultBatchConfiguration;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.batch.integration.partition.BeanFactoryStepLocator;
import org.springframework.batch.integration.partition.MessageChannelPartitionHandler;
import org.springframework.batch.integration.partition.StepExecutionRequestHandler;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.task.TaskExecutor;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.support.PeriodicTrigger;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableBatchProcessing
@EnableIntegration
@Import(JdbcDefaultBatchConfiguration.class)
public class BatchPartitionBugApplication {

    private static final CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void main(String[] args) {
        ApplicationContext context = new AnnotationConfigApplicationContext(BatchPartitionBugApplication.class);
        JobOperator jobOperator = context.getBean(JobOperator.class);
        Job job = context.getBean("controllerJob", Job.class);
        JobParameters jobParameters = new JobParametersBuilder().addString("uuid", UUID.randomUUID().toString()).toJobParameters();
        try {
            // Start the job
            Executors.newSingleThreadExecutor().submit(() -> {
                try {
                    jobOperator.start(job, jobParameters);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            JobRepository jobRepository = context.getBean(JobRepository.class);
            JobInstance jobInstance;

            // Wait for launch
            while (true) {
                jobInstance = jobRepository.getJobInstance(job.getName(), jobParameters);
                if (jobInstance == null) {
                    continue;
                }
                List<JobExecution> execution = jobRepository.getJobExecutions(jobInstance);
                if (execution.size() == 1 && execution.getFirst().getStatus().equals(BatchStatus.STARTED)) {
                    break;
                }
            }
            // Controller is shutdown, workers continue
            jobOperator.stop(jobRepository.getLastJobExecution(jobInstance));

            // Wait for controller to stop
            while (true) {
                JobExecution execution = jobRepository.getLastJobExecution(jobInstance);
                if (execution.getStatus().equals(BatchStatus.STOPPED)) {
                    break;
                }
            }

            //Unlock workers
            countDownLatch.countDown();

            // Wait for workers to complete
            while (true) {
                JobExecution execution = jobRepository.getLastJobExecution(jobInstance);
                if (execution.getStepExecutions().stream().filter(stepExecution -> stepExecution.getStatus() == BatchStatus.COMPLETED).count() == 2) {
                    break;
                }
            }

            // Restart the controller job
            jobOperator.restart(jobRepository.getLastJobExecution(jobInstance));

            // Wait for the controller to complete
            while (true) {
                JobExecution execution = jobRepository.getLastJobExecution(jobInstance);
                if (execution.getStatus().equals(BatchStatus.FAILED) || execution.getStatus().equals(BatchStatus.COMPLETED)) {
                    break;
                }
            }

            // Bug: Controller should complete successfully, but completed executions are filtered out and the aggregator is not passed the completed values
            JobExecution execution = jobRepository.getLastJobExecution(jobInstance);
            if (execution.getStatus().equals(BatchStatus.FAILED)) {
                System.err.println("BUG: Aggregator does not contain completed worker steps");
                System.exit(1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Bean("workerTasklet")
    public Tasklet workerTasklet() {
        return new Tasklet() {

            @Override
            public @Nullable RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                //Artificial delay for representing delay
                countDownLatch.await();
                return RepeatStatus.FINISHED;
            }
        };
    }

    @Bean
    public DataSource dataSource() {
        return new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .addScript("/db/schema-h2.sql")
                .build();
    }

    @Bean
    public JdbcTransactionManager transactionManager(DataSource dataSource) {
        return new JdbcTransactionManager(dataSource);
    }

    @Bean
    public PartitionHandler partitionHandler() throws Exception {
        MessageChannelPartitionHandler handler = new MessageChannelPartitionHandler();

        handler.setStepName("workerStep");
        handler.setMessagingOperations(new MessagingTemplate(outboundMessageChannel()));
        handler.setReplyChannel(workerChannel());
        handler.setGridSize(4);
        handler.setPollInterval(1000L);
        handler.setTimeout(300000L);

        return handler;
    }


    @Bean
    public MessageChannel outboundMessageChannel() {
        return new DirectChannel();
    }

    @Bean
    public PollableChannel workerChannel() {
        return new QueueChannel();
    }


    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerMetadata defaultPoller() {
        PollerMetadata pollerMetadata = new PollerMetadata();
        pollerMetadata.setTrigger(new PeriodicTrigger(10));
        pollerMetadata.setMaxMessagesPerPoll(5);
        return pollerMetadata;
    }

    @Bean
    public StepExecutionRequestHandler stepExecutionRequestHandler(JobRepository jobRepository) throws Exception {
        StepExecutionRequestHandler stepExecutionRequestHandler = new StepExecutionRequestHandler();
        stepExecutionRequestHandler.setStepLocator(beanFactoryStepLocator());
        stepExecutionRequestHandler.setJobRepository(jobRepository);
        return stepExecutionRequestHandler;
    }


    @Bean
    public BeanFactoryStepLocator beanFactoryStepLocator() {
        return new BeanFactoryStepLocator();
    }


    @Bean
    @ServiceActivator(inputChannel = "outboundMessageChannel", outputChannel = "workerChannel")
    public StepExecutionRequestHandler serviceActivator(JobRepository jobRepository) throws Exception {
        return stepExecutionRequestHandler(jobRepository);
    }


    @Bean("taskExecutor")
    public TaskExecutor workerTaskExecutor() {
        var executor = new ThreadPoolTaskExecutor();
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setCorePoolSize(2);
        executor.setQueueCapacity(2);
        executor.setMaxPoolSize(2);
        return executor;
    }

    @Bean("controllerJob")
    public Job controllerJob(JobRepository jobRepository) throws Exception {
        return new JobBuilder("controllerJob", jobRepository)
                .start(new StepBuilder("partitioner", jobRepository)
                        .partitioner("partitioner", partitioner())
                        .partitionHandler(partitionHandler())
                        .aggregator((result, executions) -> {
                            // Should contain the two workers
                            if (executions.isEmpty()) {
                                throw new RuntimeException("BUG: Completed aggregator does not contain all executions");
                            }
                        })
                        .build())
                .build();
    }

    @Bean("workerStep")
    public Step workerStep(JobRepository jobRepository) {
        return new StepBuilder("workerStep", jobRepository)
                .tasklet(workerTasklet(), transactionManager(dataSource()))
                .build();
    }

    @Bean
    public Partitioner partitioner() {
        return new Partitioner() {
            @Override
            public Map<String, ExecutionContext> partition(int gridSize) {
                return Map.of("1", new ExecutionContext(),
                        "2", new ExecutionContext());
            }
        };
    }

}
