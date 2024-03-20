package sample.rs.service.amq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

@Component
public class AMQEventSourceEngine implements EventSource, ExceptionListener {

    private static final Logger LOG = LoggerFactory.getLogger(AMQEventSourceEngine.class);

    private static final int JMS_DELIVERY_MODE = DeliveryMode.PERSISTENT;

    @Autowired
    private ConnectionFactory connectionFactory;
    @Value("${failure.reconnect.delay}")
    private long failureReconnectDelay;

    private final Map<String, Collection<Session>> callbackSessions = new ConcurrentHashMap<>();
    private final Map<Session, MessageConsumer> consumerPerSession = new ConcurrentHashMap<>();
    private volatile Connection connection;
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();


    public void sendEvent(String queueName, String msg, Long timeToLive) throws JMSException {
        Session session = null;
        MessageProducer producer = null;
        Long oldTtl = null;
        try {
            session = createJmsSession();
            producer = session.createProducer(session.createQueue(queueName));
            producer.setDeliveryMode(JMS_DELIVERY_MODE);
            TextMessage textMsg = session.createTextMessage(msg);
            if (timeToLive != null) {
                oldTtl = producer.getTimeToLive();
                producer.setTimeToLive(timeToLive);
            }
            producer.send(textMsg);
            session.commit();
        } catch (JMSException e) {
            if (session != null) {
                try {
                    LOG.warn("Transaction rollback for {}", msg, e);
                    session.rollback();
                } catch (JMSException rbe) {
                    LOG.error("Unable to rollback the transaction for {}", msg, rbe);
                    throw e;
                }
            }
            throw e;
        } finally {
            try {
                if (null != producer) {
                    if (oldTtl != null) {
                        producer.setTimeToLive(oldTtl);
                    }
                    producer.close();
                }
            } catch (JMSException e) {
                LOG.error("Unable to close the producer", e);
            }
            try {
                if (session != null) {
                    session.close();
                }
            } catch (JMSException e) {
                LOG.error("Unable to close the session", e);
            }
        }
    }


    public void unbindAll() {
        unregisterAllEventCallbacks();
    }

    public void registerEventCallback(String queueName, EventCallback callback, int numberOfConsumers) {

        LOG.info("Starting registerEventCallback for queue {} with {} consumers", queueName, numberOfConsumers);

        final Collection<Session> sessions = callbackSessions
                .computeIfAbsent(queueName, k -> new ArrayList<>(numberOfConsumers));

        try {
            for (int i = sessions.size(); i < numberOfConsumers; ++i) {
                final Session session = createJmsSession();
                MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));
                consumer.setMessageListener(new InternalMessageListener(queueName, callback, session));
                consumerPerSession.put(session, consumer);
                sessions.add(session);
            }
            LOG.info("Registered event callback for queue {} with {} consumers", queueName, numberOfConsumers);
        } catch (JMSException e) {
            LOG.error("Unable to register event callback, retry..", e);
            scheduledExecutor.schedule(() -> {
                registerEventCallback(queueName, callback, numberOfConsumers);
            }, failureReconnectDelay, TimeUnit.SECONDS);
        }
    }

    private void unregisterEventCallback(String queueName) {

        LOG.info("Unregister event callback for queue {}", queueName);
        final Collection<Session> sessions = callbackSessions.remove(queueName);
        if (sessions != null) {
            try {
                for (Session s : sessions) {
                    s.close();
                }
            } catch (JMSException e) {
                LOG.warn("Exception during closing callback sessions for queue '" + queueName + "'", e);
            }
        }
    }

    private void unregisterAllEventCallbacks() {
        LOG.info("Unregister all event callbacks");
        for (Map.Entry<String, Collection<Session>> entry : callbackSessions.entrySet()) {
            final String queueName = entry.getKey();
            final Collection<Session> sessions = entry.getValue();
            if (sessions != null) {
                LOG.info("Unregister event callback for queue {}", queueName);
                LOG.info("Closing {} AMQ consumers...", sessions.size());
                long t1 = System.currentTimeMillis();
                for (Session s : sessions) {
                    try {
                        long tClose1 = System.currentTimeMillis();
                        consumerPerSession.remove(s).close();
                        long tClose2 = System.currentTimeMillis();
                        LOG.info("AMQ Consumer closed successfully in {}ms", tClose2 - tClose1);
                    } catch (Exception e) {
                        LOG.warn("Exception during closing consumer for queue '" + queueName + "'", e);
                    }
                }
                LOG.info("All AMQ Consumers closed in {}ms", System.currentTimeMillis() - t1);
                LOG.info("Closing {} AMQ sessions...", sessions.size());
                t1 = System.currentTimeMillis();
                for (Session s : sessions) {
                    try {
                        long tClose1 = System.currentTimeMillis();
                        s.close();
                        long tClose2 = System.currentTimeMillis();
                        LOG.info("AMQ session closed successfully in {}ms", tClose2 - tClose1);
                    } catch (Exception e) {
                        LOG.warn("Exception during closing callback sessions for queue '" + queueName + "'", e);
                    }
                }
                LOG.info("All AMQ sessions closed in {}ms", System.currentTimeMillis() - t1);
            }
        }
        callbackSessions.clear();
        consumerPerSession.clear();
    }

    @Override
    public void onException(JMSException jmsEx) {
        LOG.error("A problem with connection to ActiveMQ.", jmsEx);
    }

    // ------------------------------------------------------------------------

    Session createJmsSession() throws JMSException {
        if (null == connection) {
            synchronized (this) {
                if (null == connection) {
                    final Connection c = connectionFactory.createConnection();
                    c.setExceptionListener(this);
                    try {
                        c.start();
                        connection = c;
                    } catch (JMSException e) {
                        // close connection anyway
                        c.stop();
                        c.close();
                        throw e;
                    }
                } else {
                    LOG.info("Already connected");
                }
            }
        }

        try {
            return connection.createSession(true, Session.SESSION_TRANSACTED);
        } catch (JMSException e) {
            releaseConnection();
            LOG.warn("Closing and releasing AMQ connection because of error", e);
            throw e;
        }
    }

    // ------------------------------------------------------------------------

    private class InternalMessageListener implements MessageListener {

        private final String queueName;

        private final EventCallback callback;

        private Session session;

        InternalMessageListener(String queueName, EventCallback callback, Session session) {
            this.queueName = queueName;
            this.callback = callback;
            this.session = session;
        }

        @Override
        public void onMessage(Message message) {
            if (!(message instanceof TextMessage)) {
                LOG.error("Received JMS message is not a text message.");
                return;
            }

            final String msg;
            try {
                msg = ((TextMessage) message).getText();
            } catch (JMSException e) {
                LOG.error("Unable to retrieve data from JMS message.", e);
                return;
            }

            try {
                callback.processEvent(msg);
            } catch (Throwable th) {
                LOG.error("Unable to process an event, discarding. The event: " + msg, th);
                if (th instanceof Error) {
                    throw (Error) th;
                }
            } finally {
                acknowledge(msg);
            }
        }


        private void rollback(final String eventJson, final Throwable throwable) {
            try {
                LOG.warn("Transaction rollback for event {}", eventJson, throwable);
                if (session != null) {
                    if (session.getTransacted()) {
                        session.rollback();
                    } else {
                        LOG.warn("Session is not transacted, cannot rollback.");
                    }
                } else {
                    LOG.warn("Session is null, cannot rollback");
                }
            } catch (JMSException rbe) {
                LOG.error("Unable to rollback the transaction", rbe);
            }
        }

        private void acknowledge(final String eventJson) {
            try {
                if (session.getTransacted()) {
                    session.commit();
                }
            } catch (JMSException e) {
                LOG.error("Unable to commit the transacted message.", e);
                rollback(eventJson, e);
            }
        }
    }

    @Override
    public void removeListenerAndConnection() {
        LOG.info("AMQEventSource remove listener");
        unbindAll();
        LOG.info("AMQEventSource remove listener done");
        releaseConnection();
    }

    @Override
    public void closeConnection() {
        releaseConnection();
    }

    // CPD-OFF
    @Override
    public void closeConsumerThenConnection() {
        for (Map.Entry<String, Collection<Session>> entry : callbackSessions.entrySet()) {
            final String queueName = entry.getKey();
            final Collection<Session> sessions = entry.getValue();
            if (sessions != null) {
                LOG.info("Unregister event callback for queue {}", queueName);
                LOG.info("Closing {} AMQ consumers...", sessions.size());
                long t1 = System.currentTimeMillis();
                for (Session s : sessions) {
                    try {
                        long tClose1 = System.currentTimeMillis();
                        closeMessageConsumer(consumerPerSession.remove(s));
                        long tClose2 = System.currentTimeMillis();
                        LOG.info("AMQ Consumer closed successfully in {}ms", tClose2 - tClose1);
                    } catch (Exception e) {
                        LOG.warn("Exception during closing consumer for queue '" + queueName + "'", e);
                    }
                }
                LOG.info("All AMQ Consumers closed in {}ms", System.currentTimeMillis() - t1);
            }
        }
        releaseConnection();
    }

    public void closeMessageConsumer(MessageConsumer consumer) {
        if (consumer != null) {
            // Clear interruptions to ensure that the consumer closes successfully...
            // (working around misbehaving JMS providers such as ActiveMQ)
            boolean wasInterrupted = Thread.interrupted();
            try {
                LOG.info("Closing MessageConsumer" + consumer);
                consumer.close();
            } catch (JMSException ex) {
                LOG.warn("Could not close JMS MessageConsumer", ex);
            } catch (Throwable ex) {
                // We don't trust the JMS provider: It might throw RuntimeException or Error.
                LOG.warn("Unexpected exception on closing JMS MessageConsumer", ex);
            } finally {
                if (wasInterrupted) {
                    // Reset the interrupted flag as it was before.
                    Thread.currentThread().interrupt();
                }
            }
        }
    }


    private void releaseConnection() {
        try {
            if (connection != null) {
                LOG.info("AMQEventSource remove connection");
                connection.stop();
                LOG.info("AMQEventSource connection stop done");
                connection.close();
                LOG.info("AMQEventSource connection close done");
            }
        } catch (Exception e) {
            LOG.error("Error while stopping and closing activemq connection", e);
        } finally {
            synchronized (this) {
                connection = null;
            }
        }
    }

}
