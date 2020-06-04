package io.vertx.reactive;

import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.java.*;
import com.couchbase.client.java.env.ClusterEnvironment;
import hu.akarnokd.rxjava3.bridge.RxJavaBridge;
import io.reactivex.functions.Function;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.LoggerFormat;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.ext.web.client.HttpResponse;
import io.vertx.reactivex.ext.web.client.WebClient;
import io.vertx.reactivex.ext.web.client.predicate.ResponsePredicate;
import io.vertx.reactivex.ext.web.handler.LoggerHandler;
import lombok.extern.slf4j.Slf4j;
import reactor.adapter.rxjava.RxJava3Adapter;
import reactor.core.publisher.Flux;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;


@Slf4j
public class ServerVerticle extends AbstractVerticle {
    private static final int serverPort = 8080;

    private final String hostName = "json-server.reactive-demo.svc.cluster.local";
    private final int port = 8080;

    private WebClient webClient;

    private ReactiveCollection reactiveCollection;


    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        super.start();

        // Webclient
        webClient = WebClient.create(vertx);

        //Couchbase
        ClusterEnvironment env = ClusterEnvironment.builder().ioConfig(IoConfig.maxHttpConnections(100)).timeoutConfig(TimeoutConfig.connectTimeout(Duration.ofSeconds(120))).build();
        ReactiveCluster reactiveCluster = Cluster.connect("172.30.55.135", ClusterOptions
                .clusterOptions("reactive", "reactive")
                .environment(env)).reactive();
        ReactiveBucket reactiveBucket = reactiveCluster.bucket("reactive");
        reactiveCollection = reactiveBucket.defaultCollection();
        try {
            // Make the dummy couchbase call to start the client
            reactiveCollection.get("GARBAGE").block();
        } catch (Exception e) {
        }

        // Insert the test data
        Flux
                .range(0, 101)
                .flatMap(counter -> {
                    return reactiveCollection
                            .upsert(String.valueOf(counter), com.couchbase.client.java.json.JsonObject.create()
                                    .put("host", hostName)
                                    .put("port", port)
                                    .put("identifier", "/" + String.valueOf(counter)));
                })
                .last()
                .subscribe();

        // Router
        Router router = Router.router(vertx);

        router.route().handler(LoggerHandler.create(LoggerFormat.TINY));

        router.get("/health").handler(this::healthHandler);

        router.get("/rxjava3").handler(this::rxjava3);

        router.get("/mutiny").handler(this::mutiny);

        router.get("/reactor").handler(this::reactor);

        // HTTP Server
        vertx.createHttpServer(new HttpServerOptions()
                .setTcpFastOpen(true)
                .setTcpQuickAck(true)
                .setTcpNoDelay(true)
                .setReusePort(true))
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
        log.info("vertx.prefer-native-transport: " + vertx.isNativeTransportEnabled());
    }

    private void healthHandler(RoutingContext routingContext) {
        routingContext.response().setStatusCode(200).end(new JsonObject().put("health", "ok").encode());
    }

    private void rxjava3(RoutingContext routingContext) {
        final String uuid = UUID.randomUUID().toString();
        Set<Integer> idSet = new HashSet<>();
        do {
            idSet.add((int) (Math.random() * 100));
        } while (idSet.size() != 10);
        Flowable
                .range(0, 100)
                .observeOn(Schedulers.io())
                .flatMap(id -> {
                    log.info("{} | Getting the url for id: {}", uuid, id);
                    return RxJava3Adapter.monoToSingle(reactiveCollection.get(String.valueOf(id))).observeOn(Schedulers.newThread()).map(getResult -> getResult.contentAsObject()).toFlowable();
                })
                .flatMap(jsonObject -> {
                    return RxJavaBridge.toV3Single(webClient
                            .get(jsonObject.getInt("port"),
                                    jsonObject.getString("host"),
                                    jsonObject.getString("identifier"))
                            .expect(ResponsePredicate.status(200, 202))
                            .rxSend()
                            .map(new Function<HttpResponse<Buffer>, JsonObject>() {
                                @Override
                                public JsonObject apply(HttpResponse<Buffer> bufferHttpResponse) throws Exception {
                                    log.info("{} | Received response for: {}", uuid, jsonObject.getString("url"));
                                    return bufferHttpResponse.bodyAsJsonObject();
                                }
                            }))
                            .toFlowable();
                })
                .observeOn(Schedulers.io())
                .toList()
                .map(listOfJsonResponses -> {
                    log.info("{} | Creating the json array for the responses received", uuid);
                    JsonArray jsonArray = new JsonArray();
                    for (JsonObject jsonObject : listOfJsonResponses) {
                        jsonArray.add(jsonObject);
                    }
                    return jsonArray;
                })
                .subscribe(results ->
                                routingContext.response().setStatusCode(200).putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(results.encodePrettily())
                        , error ->
                                routingContext.response().setStatusCode(500).putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(new JsonObject().put("error", getStackTrace(error)).encodePrettily())
                );
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
        return sw;
    }
}
