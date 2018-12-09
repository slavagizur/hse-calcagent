package ru.ibs.education.calcagent.model;

/**
 * Результат расчета.
 *
 * @author vbotalov
 */
public class CalculationResponse {

    /** Результат расчета. */
    private double result;

    /** Был ли успешен расчет. */
    private boolean success;

    /** Сообщение об ошибке (только если success = false). */
    private String message;

    /** Кем был выполнен расчет. */
    private String executor;

    /** Исходный запрос. */
    private String sourceRequest;

    public CalculationResponse(String executor) {
        this.executor = executor;
    }

    public CalculationResponse() {
    }

    public double getResult() {
        return result;
    }

    public void setResult(double result) {
        this.result = result;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getExecutor() {
        return executor;
    }

    public void setExecutor(String executor) {
        this.executor = executor;
    }

    public String getSourceRequest() {
        return sourceRequest;
    }

    public void setSourceRequest(String sourceRequest) {
        this.sourceRequest = sourceRequest;
    }
}
