package com.jackhenry.dkleinschmidt;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;

public class App implements AutoCloseable {

    static Statement statement = Statement.of("DELETE FROM Singers WHERE SingerId = 0");

    DatabaseClient databaseClient;
    Spanner spanner;
    ExecutorService threadPool;

    public App(DatabaseId databaseId) {
        threadPool = Executors.newCachedThreadPool();
        spanner = SpannerOptions.getDefaultInstance().getService();
        databaseClient = spanner.getDatabaseClient(databaseId);
    }

    public void close() {
        spanner.close();
        threadPool.shutdown();
    }

    public int run() throws InterruptedException, ExecutionException {
        return databaseClient.runAsync()
                .runAsync(
                        txn -> {
                            List<ApiFuture<Long>> listFutures = Stream
                                    .generate(() -> txn.executeUpdateAsync(statement))
                                    .limit(16)
                                    .toList();
                            ApiFuture<List<Long>> futureList = ApiFutures.allAsList(listFutures);

                            return ApiFutures.transform(futureList, List::size, threadPool);
                        },
                        threadPool)
                .get();
    }

    public static void main(String[] args) {
        try {
            DatabaseId databaseId = DatabaseId.of(args[0], args[1], args[2]);

            try (App app = new App(databaseId)) {
                app.run();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        } catch (ArrayIndexOutOfBoundsException arrayIndexOutOfBounds) {
            System.err.println("Usage:");
            System.err.println("./gradlew run --args=\"projectId instanceId databaseId\"");
        }
    }
}
