package ru.ibs.education.calcagent.services;

import org.springframework.beans.factory.annotation.Autowired;
import ru.ibs.education.calcagent.kafka.Consumer;

/**
 * Сервис запуска считывания данных из Kafka.
 *
 * @author vbotalov
 */
public class KafkaService implements Runnable {

    @Autowired
    private Consumer consumer;

    @Override
    public void run() {
        consumer.receiveInBackground();
    }
}
