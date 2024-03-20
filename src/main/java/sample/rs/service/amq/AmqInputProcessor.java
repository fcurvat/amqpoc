package sample.rs.service.amq;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.jms.JMSException;

@Component
public class AmqInputProcessor implements EventCallback {

    private static final Logger LOG = LoggerFactory.getLogger(AmqInputProcessor.class);

    private AtomicInteger atomicInteger = new AtomicInteger();
    private long timeStart;

    @Override
    public void processEvent(String msg) throws JMSException {
        int wait = 20000;
        LOG.info("## processing {} event {} and wait {}", atomicInteger.incrementAndGet(), msg, wait);
        try {
            Thread.sleep(wait);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
