import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class Consumer {
    final static Logger log = LoggerFactory.getLogger(Consumer.class);

    public static String RESERVATION_SOURCE = "Reservation";
    public static String TXN_SOURCE = "Txn";

    String name;
    Random rnd = new Random(System.nanoTime());

    public Consumer(String name) {
        this.name = name;
    }

    public String processEvent(String event) {
        try {
            Thread.sleep(rnd.nextInt(50) * 100);
        } catch (InterruptedException ignore) {}
        log.info("[{}] Persisted: {}", name, event);
        return event;
    }

    public List<String> processEvents(List<String> events) {
        try {
            Thread.sleep(rnd.nextInt(50) * 100);
        } catch (InterruptedException ignore) {}
        log.info("[{}] Persisted: {}", name, events);
        return events;
    }
}
