/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package sample.rs.service;
import java.util.Arrays;

import javax.jms.JMSException;

import org.apache.cxf.Bus;
import org.apache.cxf.endpoint.Server;
import org.apache.cxf.ext.logging.LoggingFeature;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.jaxrs.openapi.OpenApiFeature;
import org.apache.cxf.jaxrs.swagger.ui.SwaggerUiConfig;
import org.apache.cxf.metrics.MetricsFeature;
import org.apache.cxf.metrics.MetricsProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;


import jakarta.annotation.PostConstruct;
import sample.rs.service.amq.AmqInputProcessor;
import sample.rs.service.amq.EventCallback;
import sample.rs.service.amq.EventSource;
import sample.rs.service.hello1.HelloServiceImpl1;

@SpringBootApplication
public class PocAmqApplication {
    @Autowired
    private Bus bus;
    @Autowired
    private MetricsProvider metricsProvider;
    @Autowired
    private EventSource eventSource;
    @Autowired
    private AmqInputProcessor amqInputProcessor;


    @Value("${queue.input.name}")
    private String queueInputName;

    @Value("${queue.input.consumers}")
    private Integer queueInputConsumers;

    public static void main(String[] args) {
        SpringApplication.run(PocAmqApplication.class, args);
    }

    @Bean
    public Server rsServer() {
        JAXRSServerFactoryBean endpoint = new JAXRSServerFactoryBean();
        endpoint.setBus(bus);
        endpoint.setServiceBeans(Arrays.<Object>asList(new HelloServiceImpl1()));
        endpoint.setAddress("/");
        endpoint.setFeatures(Arrays.asList(createOpenApiFeature(), metricsFeature(), new LoggingFeature()));
        return endpoint.create();
    }

    @Bean
    public OpenApiFeature createOpenApiFeature() {
        final OpenApiFeature openApiFeature = new OpenApiFeature();
        openApiFeature.setPrettyPrint(true);
        openApiFeature.setTitle("Spring Boot CXF REST Application");
        openApiFeature.setContactName("The Apache CXF team");
        openApiFeature.setDescription("This sample project demonstrates how to use CXF JAX-RS services"
                + " with Spring Boot. This demo has two JAX-RS class resources being"
                + " deployed in a single JAX-RS endpoint.");
        openApiFeature.setVersion("1.0.0");
        openApiFeature.setSwaggerUiConfig(
            new SwaggerUiConfig()
                .url("/services/helloservice/openapi.json"));
        return openApiFeature;
    }
    
    @Bean
    public MetricsFeature metricsFeature() {
        return new MetricsFeature(metricsProvider);
    }


    @Bean
    public EventCallback eventCallback() {
        eventSource.registerEventCallback(queueInputName, amqInputProcessor, queueInputConsumers);
        return amqInputProcessor;
    }

    @PostConstruct
    public void init() throws JMSException {
        for (int i = 0; i < 1000; i++) {
            eventSource.sendEvent(queueInputName, "Message #" + i, null);
        }
    }

}
