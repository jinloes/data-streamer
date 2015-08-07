package com.jinloes.data_streamer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by rr2re on 8/7/2015.
 */
public class Server extends AbstractVerticle {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new Server());
        /*vertx.deployVerticle(new CsvReader());
        vertx.deployVerticle(new JsonWriter());*/
        vertx.eventBus().registerDefaultCodec(Document.class, new DocumentCodec());
    }

    @Override
    public void start() {
        EventBus eventBus = vertx.eventBus();
        Router router = Router.router(vertx);

        router.route().handler(BodyHandler.create());
        router.post("/start").handler(routingContext -> {
            StreamerVerticle csv = new Reader("CsvReader", new CsvReader(), "JsonReader");
            StreamerVerticle json = new StreamerVerticle("JsonReader", new JsonWriter(), null);
            final String[] deployId = {null, null};
            vertx.deployVerticle(csv, new Handler<AsyncResult<String>>() {
                @Override
                public void handle(AsyncResult<String> event) {
                    deployId[0] = event.result();
                }
            });
            vertx.deployVerticle(json, new Handler<AsyncResult<String>>() {
                @Override
                public void handle(AsyncResult<String> event) {
                    deployId[1] = event.result();
                }
            });
            eventBus.send("CsvReader", Document.of(null));
            //vertx.undeploy(deployId[0]);
            //vertx.undeploy(deployId[1]);
            System.out.println("Removed verticles");
            HttpServerResponse response = routingContext.response();
            response.setStatusCode(200);
        });
        vertx.createHttpServer().requestHandler(router::accept).listen(8081);
    }

    private static class DocumentCodec implements MessageCodec<Document, Document> {
        private ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void encodeToWire(Buffer buffer, Document document) {
            try {
                buffer.appendBytes(objectMapper.writeValueAsBytes(document));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }

        @Override
        public Document decodeFromWire(int pos, Buffer buffer) {
            try {
                return objectMapper.readValue(buffer.getString(0, buffer.length()), Document.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Document transform(Document document) {
            return document;
        }

        @Override
        public String name() {
            return "DocumentCodec";
        }

        @Override
        public byte systemCodecID() {
            return -1;
        }
    }
}
