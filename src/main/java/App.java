import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    final static Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        log.info("Starting the app");

        EventProcessor processor = new EventProcessor();
        EventSource src1 = new EventSource(Consumer.RESERVATION_SOURCE, processor);
        EventSource src2 = new EventSource(Consumer.TXN_SOURCE, processor);

        src1.start();
        src2.start();
        processor.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered.");
            src2.stop();
            src1.stop();
            processor.stop();
            log.info("Done the app.");
        }));
    }
}
