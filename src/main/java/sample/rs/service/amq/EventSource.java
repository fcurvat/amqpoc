package sample.rs.service.amq;

import javax.jms.JMSException;

/**
 * Represents an abstract source of events.
 *
 */
public interface EventSource {


    void sendEvent(String queue, String msg, Long timeToLive) throws JMSException;

    void unbindAll();

    void registerEventCallback(String queueName, EventCallback callback, int numberOfConsumers);

    /*
     * Remove all the listeners and stopping the connection.
     */
    void removeListenerAndConnection();

    void closeConnection();

    void closeConsumerThenConnection();

}
