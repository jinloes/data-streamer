package com.jinloes.data_streamer;

import com.jinloes.data_streamer.util.DocumentCodec;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

/**
 * Created by rr2re on 8/7/2015.
 */
public class Server extends AbstractVerticle {
    private final int port;

    public Server(int port) {
        this.port = port;
    }

    public static void main(String[] args) {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        Vertx.clusteredVertx(new VertxOptions().setHAEnabled(true), vertxAsyncResult -> {
            Vertx vertx = vertxAsyncResult.result();
            vertx.deployVerticle(new Server(port));
            vertx.eventBus().registerDefaultCodec(Document.class, new DocumentCodec());
        });

    }

    @Override
    public void start() {
        EventBus eventBus = vertx.eventBus();
        Router router = Router.router(vertx);

        router.route().handler(BodyHandler.create());
        router.post("/deploy").handler(routingContext -> {
            StreamerVerticle csv = new ReaderVerticle("CsvReader", new CsvReader(), "JsonReader");
            StreamerVerticle json = new WriterVerticle("JsonReader", new JsonWriter());
            final String[] deployId = {null, null};
            vertx.deployVerticle(csv, event -> {
                deployId[0] = event.result();
            });
            vertx.deployVerticle(json, event -> {
                deployId[1] = event.result();
            });
            HttpServerResponse response = routingContext.response();
            response.setStatusCode(201);
            response.end();
        });
        router.post("/start").handler(routingContext -> {
            eventBus.send("CsvReader", Document.startDocument());
            //vertx.undeploy(deployId[0]);
            //vertx.undeploy(deployId[1]);
            //System.out.println("Removed verticles");
            HttpServerResponse response = routingContext.response();
            response.setStatusCode(200);
            response.end();
        });
        vertx.createHttpServer().requestHandler(router::accept).listen(port);
    }
}
