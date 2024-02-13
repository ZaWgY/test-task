package tinkoff.task2;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DefaultHandler implements Handler {

    private static final Duration RETRY_PERIOD = Duration.ofMillis(15_000L);
    private static final int POOL_SIZE = 10;

    private final Client client;
    private final ScheduledExecutorService executorService;

    public DefaultHandler(final Client client) {
        this.client = client;
        this.executorService = Executors.newScheduledThreadPool(POOL_SIZE);
    }

    @Override
    public Duration timeout() {
        return RETRY_PERIOD;
    }

    @Override
    public void performOperation() {
        while (true) {
            final Event event = client.readData();
            for (Address recipient : event.recipients()) {
                final Payload payload = event.payload();
                sendAsync(recipient, payload);
            }
        }
    }

    private void sendAsync(final Address address, final Payload payload) {
        CompletableFuture.supplyAsync(() -> client.sendData(address, payload), executorService)
                .thenAccept(result -> manageResult(result, address, payload));
    }

    private void sendAsyncDelayed(final Address address,final  Payload payload) {
        CompletableFuture.supplyAsync(() -> client.sendData(address, payload), CompletableFuture.delayedExecutor(timeout().toMillis(), TimeUnit.MILLISECONDS, executorService))
                .thenAccept(result -> manageResult(result, address, payload));
    }

    private void manageResult(final Result result,final  Address address,final  Payload payload) {
        if (Result.REJECTED.equals(result)) {
            sendAsyncDelayed(address, payload);
        }
    }
}
