import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EventProcessor implements EventListener {
    final static Logger log = LoggerFactory.getLogger(EventProcessor.class);

    long PROCESSING_INTERVAL_MS = 30_000;
    long PROCESSING_TIMEOUT_MS = 60_000;
    int MAX_PROCESSING_ELEMENTS = 20;

    FinalConsumer finalConsumer = new FinalConsumer();
    Consumer resConsumer = new Consumer("ReservationDB");
    Consumer txnConsumer = new Consumer("TxnDB");

    ScheduledFuture<?> workerFuture;

    LinkedBlockingQueue<String> eventsQueue = new LinkedBlockingQueue<>();
    ScheduledExecutorService worker = Executors.newScheduledThreadPool(1, r -> new Thread(r, "EventProcessor.Worker"));
    ExecutorService processingService = Executors.newScheduledThreadPool(1, r -> new Thread(r, "EventProcessor.Process"));


    @Override
    public void onEvent(String event) {
        log.info("Enqueue event: {}", event);
        eventsQueue.offer(event);
    }

    public void start() {
        workerFuture = worker.scheduleAtFixedRate(this::processCurrentEvents, PROCESSING_INTERVAL_MS, PROCESSING_INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("Event processor worker has been started.");
    }

    public void stop() {
        worker.shutdown();
        processingService.shutdown();

        log.info("Stopped event processor worker.");
    }

    private void processCurrentEvents() {
        Collection<String> events = new ArrayList<>(MAX_PROCESSING_ELEMENTS);
        int size = eventsQueue.drainTo(events, MAX_PROCESSING_ELEMENTS);

        log.info(">>>>>>> Processing queued events. Found {} events. new queue size: {}", size, eventsQueue.size());

        Map<String, List<String>> grouppedEvents = events.stream().collect(Collectors.groupingBy(s -> s.split("\\.")[1]));

        List<String> reservationEvents = grouppedEvents.get(Consumer.RESERVATION_SOURCE);
        List<String> txnEvents = grouppedEvents.get(Consumer.TXN_SOURCE);

        log.info("In batch of {} events found: {} reservations and {} txns. Submit processing for them.", size, reservationEvents.size(), txnEvents.size());
        Future<List<String>> resFuture = processingService.submit(() -> resConsumer.processEvents(reservationEvents));
        Future<List<String>> txnFuture = processingService.submit(() -> txnConsumer.processEvents(txnEvents));

        List<String> allProcessedEvents = Stream.of(resFuture, txnFuture)
                .map(this::getProcessingResult)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        log.info("Got {} total results of all events. Going to final.", allProcessedEvents.size());

        finalConsumer.doFinalProcess(allProcessedEvents);

        log.info(" <<<<<<<<< Done with {} events processing: {}", size, allProcessedEvents);
    }

    private List<String> getProcessingResult(Future<List<String>> future) {
        try {
            return future.get(PROCESSING_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {}

        return Collections.emptyList();
    }

}
