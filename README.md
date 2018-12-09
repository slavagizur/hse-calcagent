# Агент для расчетов

Реализация агента расчетов через Kafka.

## Настройка и развертывание

Необходимо заполнить корректное значение kafka.url (либо в application.properties либо через параметра запуска JVM).

Для работы необходимы созданные очереди в Kafka. Для создания можно воспользоваться командами:

```
kafka-topics.sh --create --zookeeper zookeeper:2181 --topic calcReq --partitions 1 --replication-factor 1
kafka-topics.sh --create --zookeeper zookeeper:2181 --topic calcRes --partitions 1 --replication-factor 1
```

где **zookeeper:2181** это адрес кластера Zookeeper.

Имя агента можно поменять с помощью свойства **calc.executor**.