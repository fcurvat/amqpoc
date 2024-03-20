package sample.rs.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import sample.rs.service.amq.EventSource;


@Component
public class ShutdownEventSource {

    private static final Logger LOG = LoggerFactory.getLogger(ShutdownEventSource.class);

    private boolean isShuttingDown;

    @Autowired(required = false)
    private EventSource eventSource;



    @EventListener(ContextClosedEvent.class)
    public void onApplicationEvent() {
        if (isShuttingDown) {
            LOG.info("ShutdownEventSource already started");
            return;
        }

        isShuttingDown = true;

        LOG.info("ShutdownEventSource start");
        if (null != eventSource) {
            // historically what we do
            // close consumers, then close sessions
            eventSource.unbindAll();

            //eventSource.closeConsumerThenConnection();
            //eventSource.closeConnection();
        }
    }
}
