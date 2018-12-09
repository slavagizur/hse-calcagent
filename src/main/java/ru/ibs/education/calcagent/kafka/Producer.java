package ru.ibs.education.calcagent.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Записыватель в очередь.
 *
 * @author vbotalov
 */
public class Producer {

    private Properties properties;

    private String topic;

    /**
     * Инициализация записывателя.
     *
     * @param topic имя очереди;
     * @param url   URL Kafka.
     */
    public Producer(String topic, String url) {
        this.topic = topic;
        properties = new Properties();
        properties.put("bootstrap.servers", url);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public void send(String message, String key) {
        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        try {
            kafkaProducer.send(new ProducerRecord(topic, key, message));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }

}
