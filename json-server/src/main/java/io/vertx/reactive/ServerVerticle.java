package io.vertx.reactive;

import hu.akarnokd.rxjava3.bridge.RxJavaBridge;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.functions.Function;
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
import io.vertx.reactivex.ext.web.handler.LoggerHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.*;

@Slf4j
public class ServerVerticle extends AbstractVerticle {
    private static final int serverPort = 8080;
    private final int port = 8080;
    private final List<JsonObject> listOfJson = new ArrayList<>();

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        super.start();

        for(int i=0;i<=100; i++) {
            listOfJson.add(new JsonObject().put("id", i).put("UUID", UUID.randomUUID().toString()));
        }

        Router router = Router.router(vertx);

        router.route().handler(LoggerHandler.create(LoggerFormat.SHORT));

        router.get("/health").handler(this::healthHandler);

        router.get("/:identifier").handler(this::getJson);

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

    private void getJson(RoutingContext routingContext) {
        try{
            Integer counter = Integer.parseInt(routingContext.pathParam("identifier").trim());
        } catch (NumberFormatException nfe) {
            routingContext.response().setStatusCode(404).end(new JsonObject().put("error", "Invalid identifier").encode());
            return;
        }
        routingContext.response().setStatusCode(200).end(listOfJson.get(Integer.parseInt(routingContext.pathParam("identifier").trim())).encode());
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
