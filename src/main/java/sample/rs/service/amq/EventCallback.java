package sample.rs.service.amq;

import javax.jms.JMSException;

public interface EventCallback {

    String QUEUE_NAME = "queueName";
    String NUMBER_OF_CONSUMERS = "numberOfConsumers";

    void processEvent(String msg ) throws JMSException;

}