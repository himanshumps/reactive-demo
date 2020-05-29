package io.vertx.reactive;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ServerVerticle extends AbstractVerticle {
    private static final int serverPort = 8080;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        super.start();
        Router router = Router.router(vertx);
        router.get("/health").handler(this::healthHandler);
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

    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {
        stopPromise.complete();
    }
}
