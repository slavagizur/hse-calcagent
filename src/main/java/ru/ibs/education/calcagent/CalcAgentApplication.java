package ru.ibs.education.calcagent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import ru.ibs.education.calcagent.kafka.Consumer;
import ru.ibs.education.calcagent.kafka.Producer;
import ru.ibs.education.calcagent.model.CalculationRequest;
import ru.ibs.education.calcagent.model.CalculationResponse;
import ru.ibs.education.calcagent.services.KafkaCalculationService;
import ru.ibs.education.calcagent.services.KafkaService;

import javax.annotation.Resource;

/**
 * Инициализация и запуск агента расчетов.
 *
 * @author vbotalov
 */
@SpringBootApplication
public class CalcAgentApplication {

    private static final String KAFKA_URL = "kafka.url";

    @Resource
    private Environment environment;

    public static void main(String[] args) {
        SpringApplication.run(CalcAgentApplication.class, args);
    }

    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    @Bean
    public CommandLineRunner schedulingRunner(TaskExecutor taskExecutor, KafkaService kafkaService) {
        return args -> taskExecutor.execute(kafkaService);
    }

    @Bean
    public KafkaService kafkaService() {
        return new KafkaService();
    }

    @Bean
    public Producer producer() {
        return new Producer(environment.getProperty("calc.result"),
                environment.getProperty(KAFKA_URL));
    }

    @Bean
    public Consumer consumer(KafkaCalculationService kafkaCalculationService) {
        return new Consumer(environment.getProperty("calc.request"),
                environment.getProperty(KAFKA_URL),
                kafkaCalculationService);
    }

    @Bean
    public KafkaCalculationService kafkaCalculationService() {
        return new KafkaCalculationService();
    }

    @Bean
    @Qualifier("kafka-executor")
    public String executor() {
        return environment.getProperty("calc.executor");
    }

    @Bean
    public ObjectMapper mapper() {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule calculatorModule = new SimpleModule();
        calculatorModule.registerSubtypes(CalculationRequest.class, CalculationResponse.class);
        mapper.registerModule(calculatorModule);
        return mapper;
    }
}
