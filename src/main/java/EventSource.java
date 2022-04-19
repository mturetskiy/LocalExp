import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class EventSource {
    final static Logger log = LoggerFactory.getLogger(EventSource.class);

    long THREAD_STOP_TIMEOUT_MS = 1000;

    String name;
    EventListener listener;
    private volatile boolean active;
    Random rnd = new Random(System.nanoTime());
    AtomicLong idGen = new AtomicLong(0);
    Thread thread;

    public EventSource(String name, EventListener listener) {
        this.name = name;
        this.listener = listener;
    }

    public void start() {
        active = true;
        thread = new Thread(() -> {
            while(active) {
                String event = "Evt." + name + "." + idGen.getAndIncrement();
                log.info("[{}] Generating event: {}", name, event);
                listener.onEvent(event);
                try {
                    Thread.sleep(100L * rnd.nextInt(1, 10));
                } catch (InterruptedException e) {
                    log.error("[{}] main loop has been interrupted. Deactivating.", name);
                    active = false;
                    Thread.currentThread().interrupt();
                }
            }

            log.info("[{}] Stopped event source", name);
        }, "EventSource." + name);
        thread.start();

        log.info("[{}] Started event source.", name);
    }

    public void stop() {
        active = false;
        if (thread != null) {
            try {
                thread.join(THREAD_STOP_TIMEOUT_MS);
            } catch (InterruptedException e) {}
        }
    }
}
