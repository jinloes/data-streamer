package com.jinloes.data_streamer;

import com.jinloes.data_streamer.util.DocumentCodec;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

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
        int mongoPort = startMongo();
        Vertx.clusteredVertx(new VertxOptions().setHAEnabled(true), vertxAsyncResult -> {
            Vertx vertx = vertxAsyncResult.result();
            MongoClient client = MongoClient.createShared(vertx, new JsonObject().put("port", mongoPort));
            vertx.deployVerticle(new Server(port, client));
            vertx.eventBus().registerDefaultCodec(Document.class, new DocumentCodec());
        });
    }

    private static int startMongo() throws IOException {
        int mongoPort = Network.getFreeServerPort();
        MongodStarter starter = MongodStarter.getDefaultInstance();
        IMongodConfig mongodConfig = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(mongoPort, Network.localhostIsIPv6()))
                .build();
        MongodExecutable mongodExecutable = starter.prepare(mongodConfig);
        mongodExecutable.start();
        return mongoPort;
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
            UUID streamId = UUID.randomUUID();
            LOGGER.info("Stream id: " + streamId);
            eventBus.send("saveStream", new JsonObject().put("stream_id", streamId.toString()));
            eventBus.send("CsvReader", Document.startDocument());
            //vertx.undeploy(deployId[0]);
            //vertx.undeploy(deployId[1]);
            //System.out.println("Removed verticles");
            HttpServerResponse response = routingContext.response();
            response.setStatusCode(200);
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
