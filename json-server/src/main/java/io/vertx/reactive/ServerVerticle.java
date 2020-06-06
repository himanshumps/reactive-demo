package io.vertx.reactive;

import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.LoggerFormat;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.ext.web.handler.LoggerHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


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

        router.get("/").handler(request -> {
            request.response().end("Welcome to reactive session ...");
        });

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
