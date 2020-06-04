package io.vertx.reactive;

import hu.akarnokd.rxjava3.bridge.RxJavaBridge;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.functions.Function;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.LoggerFormat;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.ext.web.client.HttpResponse;
import io.vertx.reactivex.ext.web.client.WebClient;
import io.vertx.reactivex.ext.web.handler.LoggerHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;

@Slf4j
public class ServerVerticle extends AbstractVerticle {
    private static final int serverPort = 8080;

    private final String hostName = "json-server.reactive-demo.svc.cluster.local";
    private final int port = 8080;

    private WebClient webClient;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        super.start();

        webClient = WebClient.create(vertx);

        Router router = Router.router(vertx);

        router.route().handler(LoggerHandler.create(LoggerFormat.TINY));

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
        Set<Integer> idSet = new HashSet<>();
        do {
            idSet.add((int) (Math.random() * 100));
        } while (idSet.size() != 10);
        Flowable
                .fromIterable(idSet)
                .subscribeOn(io.reactivex.rxjava3.schedulers.Schedulers.io())
                .flatMap(id -> {
                    String url = MessageFormat.format("http://{0}:{1,number,#}/{2,number,#}", hostName, port, id);
                    log.info("Calling the url: {}", url);
                    return webClient
                            .getAbs(url)
                            .rxSend()
                            .as(RxJavaBridge.toV3Single())
                            .observeOn(io.reactivex.rxjava3.schedulers.Schedulers.io())
                            .map(new Function<HttpResponse<Buffer>, JsonObject>() {
                                @Override
                                public JsonObject apply(HttpResponse<Buffer> bufferHttpResponse) throws Throwable {
                                    log.info("Received response for: {}", id);
                                    return bufferHttpResponse.bodyAsJsonObject();
                                }
                            })
                            .toFlowable();
                })
                .toList()
                .map(listOfJsonResponses -> {
                    log.info("Creating the json array for the responses received");
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
