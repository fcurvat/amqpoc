package sample.rs.service;


import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.jms.ConnectionFactory;

@Component
public class ConnectionFactoryConfiguration {

    @Value("${activemq.broker.url}")
    private String activemqBrokerUrl;
    @Value("${activemq.broker.username}")
    private String activemqBrokerUsername;
    @Value("${activemq.broker.password}")
    private String activemqBrokerPassword;
    @Value("${activemq.broker.connection.max}")
    private int activemqBrokerConnectionMax = 1;
    @Value("${activemq.broker.maximumRedeliveries:6}")
    private int maximumRedeliveries;
    @Value("${activemq.broker.useExponentialBackOff:true}")
    private boolean useExponentialBackOff;

    private PooledConnectionFactory pooledConnectionFactory;


    @Bean
    public ConnectionFactory connectionFactory() {
        return pooledConnectionFactory;
    }

    @PostConstruct
    public void init() throws Exception {
        final ActiveMQConnectionFactory activeMQConnectionFactory =
                new ActiveMQConnectionFactory(activemqBrokerUsername, activemqBrokerPassword, activemqBrokerUrl);
        activeMQConnectionFactory.setConsumerExpiryCheckEnabled(false);

        RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
        redeliveryPolicy.setMaximumRedeliveries(maximumRedeliveries);
        redeliveryPolicy.setUseExponentialBackOff(useExponentialBackOff);
        activeMQConnectionFactory.setRedeliveryPolicy(redeliveryPolicy);

        pooledConnectionFactory = new PooledConnectionFactory();
        pooledConnectionFactory.setConnectionFactory(activeMQConnectionFactory);
        pooledConnectionFactory.setMaxConnections(activemqBrokerConnectionMax);
        pooledConnectionFactory.setExpiryTimeout(10000L);
        pooledConnectionFactory.setCreateConnectionOnStartup(false);
        pooledConnectionFactory.start();
    }

    @PreDestroy
    public void destroy() throws Exception {
        pooledConnectionFactory.stop();
    }

}
