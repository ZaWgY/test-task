package tinkoff.task1;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;


public class DefaultHandler implements Handler {

    private static final long TIMEOUT_MILLIS = 15_000L;
    private static final int POOL_SIZE = 10;

    private final ScheduledExecutorService executorService;
    private final Client client;
    private final List<Function<String, Response>> applicationStatusImplementations;

    public DefaultHandler(final Client client) {
        this.client = client;
        this.executorService = Executors.newScheduledThreadPool(POOL_SIZE);
        this.applicationStatusImplementations = List.of(
                this.client::getApplicationStatus1,
                this.client::getApplicationStatus2
        );
    }

    @Override
    public ApplicationStatusResponse performOperation(final String id) {
        final long startTime = System.currentTimeMillis();
        final AtomicInteger retryCount = new AtomicInteger();
        final AtomicReference<Duration> lastFailedAttempt = new AtomicReference<>(Duration.ZERO);
        long elapsed;

        List<CompletableFuture<Response>> responses = applicationStatusImplementations.stream()
                .map(impl -> singleStatusRequest(id, impl, retryCount, lastFailedAttempt))
                .toList();

        do {
            for (final CompletableFuture<Response> response : responses) {
                if (response.isDone()) {
                    return wrappedMapResponse(response, retryCount, lastFailedAttempt);
                }
            }

            elapsed = System.currentTimeMillis() - startTime;
        } while (elapsed < TIMEOUT_MILLIS);

        return new ApplicationStatusResponse.Failure(lastFailedAttempt.get(), retryCount.get());
    }

    private ApplicationStatusResponse wrappedMapResponse(final Future<Response> responseFuture,final AtomicInteger retryCount,final AtomicReference<Duration> lastFail) {
        try {
            return mapResponse(
                    responseFuture.get(),
                    retryCount,
                    lastFail
            );
        } catch (final InterruptedException | ExecutionException e) {
            return new ApplicationStatusResponse.Failure(Duration.ofMillis(System.currentTimeMillis()), retryCount.get() + 1);
        }
    }

    private ApplicationStatusResponse mapResponse(final Response response,final AtomicInteger retryCount,final AtomicReference<Duration> lastFail) {
        if (response instanceof Response.Success success) {
            return new ApplicationStatusResponse.Success(success.applicationId(), success.applicationStatus());
        }

        if (response instanceof Response.Failure) {
            return new ApplicationStatusResponse.Failure(Duration.ofMillis(System.currentTimeMillis()), retryCount.get() + 1);
        }

        throw new IllegalArgumentException();
    }

    private CompletableFuture<Response> singleStatusRequest(
            final String id,
            final Function<String, Response> requestImpl,
            final AtomicInteger retryCounter,
            final AtomicReference<Duration> lastFail
    ) {
        return CompletableFuture.supplyAsync(() -> requestImpl.apply(id))
                .thenCompose(response -> {
                    if ( response instanceof Response.RetryAfter retryResponse) {
                        retryCounter.incrementAndGet();
                        lastFail.set(Duration.ofMillis(System.currentTimeMillis()));
                        return CompletableFuture.supplyAsync(
                                () -> requestImpl.apply(id),
                                CompletableFuture.delayedExecutor(
                                        delay(retryResponse.delay().toMillis()),
                                        TimeUnit.MILLISECONDS,
                                        executorService
                                )
                        );
                    }

                    return CompletableFuture.completedFuture(response);
                });
    }

    private long delay(final long responseDelay) {
        return Math.min(responseDelay, TIMEOUT_MILLIS);
    }

}
