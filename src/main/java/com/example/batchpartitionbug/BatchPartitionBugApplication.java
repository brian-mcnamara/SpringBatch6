package com.example.batchpartitionbug;

import org.jspecify.annotations.Nullable;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.support.JdbcDefaultBatchConfiguration;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.JobParameters;
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
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableBatchProcessing
@EnableIntegration
@Import(JdbcDefaultBatchConfiguration.class)
public class BatchPartitionBugApplication {

    public static void main(String[] args) {
        ApplicationContext context = new AnnotationConfigApplicationContext(BatchPartitionBugApplication.class);
        JobOperator jobOperator = context.getBean(JobOperator.class);
        Job job = context.getBean("controllerJob", Job.class);
        JobParameters jobParameters = new JobParameters();
        try {
            JobExecution jobExecution = jobOperator.start(job, jobParameters);
            JobRepository jobRepository = context.getBean(JobRepository.class);
            while (true) {
                JobExecution execution = jobRepository.getJobExecution(jobExecution.getId());
                if (execution.getStatus().isGreaterThan(BatchStatus.STARTED)) {
                    break;
                }
                Thread.sleep(100);
            }
            JobExecution execution = jobRepository.getJobExecution(jobExecution.getId());

            if (!execution.getFailureExceptions().isEmpty()) {
                System.err.println("BUG: context not persisted");
                execution.getFailureExceptions().forEach(it -> it.printStackTrace());
                System.exit(1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final String CONTEXT_KEY = "testKey";

    @Bean("workerTasklet")
    public Tasklet workerTasklet() {
        return new Tasklet() {

            @Override
            public @Nullable RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                // Bug is demonstrated here
                if (contribution.getStepExecution().getExecutionContext().get(CONTEXT_KEY) == null) {
                    throw new Exception("BUG: context key is null");
                }
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
                return Map.of("1", new ExecutionContext(Map.of(CONTEXT_KEY, 1)),
                        "2", new ExecutionContext(Map.of(CONTEXT_KEY, 2)));
            }
        };
    }

}
