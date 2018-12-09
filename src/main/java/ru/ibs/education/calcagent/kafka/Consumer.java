package ru.ibs.education.calcagent.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.function.BiConsumer;

/**
 * Считыватель для данных из очереди.
 */
public class Consumer {

    private static final long WAIT_TIME = 100L;

    private Properties properties;

    private String topic;

    private BiConsumer<String, String> serviceExecutor;

    /**
     * Инициализация считывателя.
     *
     * @param topic           имя очереди;
     * @param url             URL Kafka.
     * @param serviceExecutor объект для обработка, полученных из очереди данных
     */
    public Consumer(String topic, String url, BiConsumer<String, String> serviceExecutor) {
        this.serviceExecutor = serviceExecutor;
        this.topic = topic;
        properties = new Properties();
        properties.put("bootstrap.servers", url);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, getClientId());
    }

    protected String getClientId() {
        return new StringBuilder().append("serviceClient").append(new Date().getTime()).toString();
    }

    public void receiveInBackground() {
        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        List<String> topics = new ArrayList<>();
        topics.add(topic);
        kafkaConsumer.subscribe(topics);
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.of(WAIT_TIME, ChronoUnit.MILLIS));
                for (ConsumerRecord<String, String> curRecord : records) {
                    serviceExecutor.accept(curRecord.key(), curRecord.value());
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }
}
