package ru.ibs.education.calcagent.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import ru.ibs.education.calcagent.kafka.Producer;
import ru.ibs.education.calcagent.model.CalculationRequest;
import ru.ibs.education.calcagent.model.CalculationResponse;

import java.util.function.BiConsumer;

/**
 * Реализация расчетов с обменом через Kafka.
 *
 * @author vbotalov
 */
public class KafkaCalculationService implements BiConsumer<String, String> {

    @Autowired
    @Qualifier("kafka-executor")
    private String executor;

    @Autowired
    private Producer producer;

    @Autowired
    private ObjectMapper mapper;

    @Override
    public void accept(String key, String request) {

        try {
            CalculationResponse response = calculate(request);
            producer.send(mapper.writeValueAsString(response), key);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private CalculationResponse calculate(String json) {
        CalculationResponse response = new CalculationResponse(executor);
        try {
            CalculationRequest request = mapper.readValue(json, CalculationRequest.class);
            long pause = 1_000L;
            switch (request.getOperation()) {
                case "+":
                    response.setResult(request.getFirst() + request.getSecond());
                    break;
                case "-":
                    response.setResult(request.getFirst() - request.getSecond());
                    break;
                case "*":
                    response.setResult(request.getFirst() * request.getSecond());
                    pause = 5_000L;
                    break;
                case "/":
                    if (request.getSecond() == 0.0) {
                        throw new Exception("Делить на ноль нельзя");
                    }
                    response.setResult(request.getFirst() / request.getSecond());
                    pause = 5_000L;
                    break;
                default:
                    throw new Exception("Не поддерживаемая операция");
            }
            response.setSuccess(true);
            Thread.sleep(pause);

        } catch (Exception e) {
            response.setSuccess(false);
            response.setMessage(e.getMessage());
        }
        return response;
    }
}
