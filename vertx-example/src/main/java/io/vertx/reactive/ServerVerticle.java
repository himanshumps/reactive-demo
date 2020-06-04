package io.vertx.reactive;

import hu.akarnokd.rxjava3.bridge.RxJavaBridge;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.functions.Function;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.reactivex.pgclient.PgPool;
import io.vertx.reactivex.sqlclient.Row;
import io.vertx.reactivex.sqlclient.Tuple;
import io.vertx.sqlclient.PoolOptions;
import lombok.extern.slf4j.Slf4j;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ServerVerticle extends AbstractVerticle {
    private static final int serverPort = 8080;

    private final String host = "postgresql.reactive-demo.svc.cluster.local";
    private final String username = "postgresql";
    private final String password = "postgresql";
    private final String database = "sampledb";
    private PgPool client;


    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        super.start();

        PgConnectOptions connectOptions = new PgConnectOptions()
                .setPort(5432)
                .setHost(host)
                .setDatabase(database)
                .setUser(username)
                .setPassword(password);
        PoolOptions poolOptions = new PoolOptions().setMaxSize(100);

        // Create the client pool
        client = PgPool.pool(connectOptions, poolOptions);

        Router router = Router.router(vertx);

        router.get("/health").handler(this::healthHandler);

        router.get("/rxjava3").handler(this::rxjava3);

        router.get("/mutiny").handler(this::mutiny);

        router.get("/reactor").handler(this::reactor);

        vertx.createHttpServer()
                .requestHandler(router)
                .listen(serverPort, result -> {

                    if (result.succeeded()) {
                        log.info("Server started at port: {0}", serverPort);
                        startPromise.complete();
                    } else {
                        log.error("Server failed to start at port " + serverPort, result.cause());
                        startPromise.fail(result.cause());
                    }
                });

    }

    private void healthHandler(RoutingContext routingContext) {
        routingContext.response().setStatusCode(200).end(new JsonObject().put("health", "ok").encode());
    }

    private void rxjava3(RoutingContext routingContext) {
        client
                .query("select ID from ids order by random() limit 10")
                .rxExecute()
                .as(RxJavaBridge.toV3Single())
                .toFlowable()
                .map(rows -> {
                    List<Integer> integers = new ArrayList<>();
                    for (Row row : rows) {
                        integers.add(row.getInteger(0));
                    }
                    return integers;
                })
                .flatMapIterable(ids -> ids)
                .flatMap(new Function<Integer, Flowable<JsonObject>>() {
                    @Override
                    public Flowable<JsonObject> apply(Integer id) throws Throwable {
                        return client.preparedQuery("select info from details where id=$1").rxExecute(Tuple.of(id)).as(RxJavaBridge.toV3Single()).toFlowable().flatMap(rows -> {
                            io.reactivex.rxjava3.core.Flowable<JsonObject> flowable = Flowable.error(new RuntimeException("No value found for " + id + " in details table"));
                            for (Row row : rows) {
                                flowable = Flowable.<JsonObject>just(row.get(JsonObject.class, 0));
                            }
                            return flowable;
                        });
                    }
                })
                .toList()
                .subscribe(results -> {
                            JsonArray jsonArray = new JsonArray();
                            for(JsonObject jsonObject : results) {
                                jsonArray.add(jsonObject);
                            }
                            routingContext.response().setStatusCode(200).end(jsonArray.encode());
                        },
                        error -> {
                            routingContext.response().setStatusCode(500).end(getStackTrace(error).toString());
                        });
    }

    private void mutiny(RoutingContext routingContext) {
    }

    private void reactor(RoutingContext routingContext) {
    }

    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {
        stopPromise.complete();
    }

    private StringWriter getStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
    }
}
