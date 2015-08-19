package com.jinloes.data_streamer;

import com.jinloes.data_streamer.util.DocumentCodec;
import de.flapdoodle.embed.process.collections.Collections;
import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by rr2re on 8/7/2015.
 */
public class Server extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);
    private final int port;
    private final MongoClient mongoClient;

    public Server(int port, MongoClient mongoClient) {
        this.port = port;
        this.mongoClient = mongoClient;
    }

    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        Vertx.clusteredVertx(new VertxOptions().setHAEnabled(true), vertxAsyncResult -> {
            Vertx vertx = vertxAsyncResult.result();
            MongoClient client = MongoClient.createShared(vertx, new JsonObject());
            vertx.deployVerticle(new Server(port, client));
            vertx.eventBus().registerDefaultCodec(Document.class, new DocumentCodec());
        });
    }

    @Override
    public void start() {
        EventBus eventBus = vertx.eventBus();
        Router router = Router.router(vertx);
        createRoutes(eventBus, router);
        vertx.deployVerticle(new StreamService(mongoClient));
        vertx.createHttpServer().requestHandler(router::accept).listen(port);
    }

    private void createRoutes(EventBus eventBus, Router router) {
        router.route().handler(BodyHandler.create());
        router.post("/streams").handler(routingContext -> {
            JsonObject stream = routingContext.getBodyAsJson();
            String streamId = UUID.randomUUID().toString();
            stream.put("stream_id", streamId);
            eventBus.send("saveStream", stream, result -> {
                HttpServerResponse response = routingContext.response();
                if (result.succeeded()) {
                    response.setStatusCode(201);
                    response.end(new JsonObject().put("id", streamId).encodePrettily());
                } else {
                    response.setStatusCode(500);
                    response.end(new JsonObject().put("message", result.cause().getMessage()).encodePrettily());
                }

            });
        });
        router.post("/deploy").handler(routingContext -> {
            StreamerVerticle csv = new ReaderVerticle("CsvReader", new CsvReader(),
                    Collections.newArrayList("JsonReader"));
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
        router.post("/start/:streamId").handler(routingContext -> {
            String streamId = routingContext.request().getParam("streamId");
            eventBus.<JsonObject>send("getStream", streamId, result -> {
                JsonObject stream = result.result().body();
                JsonArray streamers = stream.getJsonArray("streamers");
                List<String> readers = new ArrayList<>();
                Map<String, String> streamerToInstanceMap = new HashMap<>();
                for (int i = 0; i < streamers.size(); i++) {
                    JsonObject streamer = streamers.getJsonObject(i);
                    String type = streamer.getString("type");
                    String id = streamer.getString("id");
                    if (!streamerToInstanceMap.containsKey(id)) {
                        streamerToInstanceMap.put(id, UUID.randomUUID().toString());
                    }
                    switch (type) {
                        case "reader":
                            readers.add(id);
                            JsonArray outputs = streamer.getJsonArray("outputs");
                            List<String> outputAddresses = new ArrayList<>();
                            for (int j = 0; j < outputs.size(); j++) {
                                String outputId = outputs.getString(j);
                                if (!streamerToInstanceMap.containsKey(outputId)) {
                                    streamerToInstanceMap.put(outputId, UUID.randomUUID().toString());
                                }
                                outputAddresses.add(streamerToInstanceMap.get(outputId));
                            }
                            StreamerVerticle csv = new ReaderVerticle(streamerToInstanceMap.get(id),
                                    new CsvReader(), outputAddresses);
                            vertx.deployVerticle(csv);
                            break;
                        case "writer":
                            StreamerVerticle json = new WriterVerticle(streamerToInstanceMap.get(id), new JsonWriter());
                            vertx.deployVerticle(json);
                            break;
                    }
                }
                for (String reader : readers) {
                    eventBus.send(streamerToInstanceMap.get(reader), Document.startDocument());
                }
            });
            HttpServerResponse response = routingContext.response();
            response.setStatusCode(201);
            response.end();
        });
        router.get("/streamruns/:streamId").handler(routingContext ->
                eventBus.send("getStream", routingContext.request().getParam("streamId"),
                        new Handler<AsyncResult<Message<JsonObject>>>() {
                            @Override
                            public void handle(AsyncResult<Message<JsonObject>> result) {
                                HttpServerResponse response = routingContext.response();
                                response.end(result.result().body().encode());
                            }
                        }));
    }
}
