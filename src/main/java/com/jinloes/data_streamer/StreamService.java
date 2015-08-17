package com.jinloes.data_streamer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

/**
 * Created by jinloes on 8/17/15.
 */
public class StreamService extends AbstractVerticle {
    private final MongoClient mongoClient;

    public StreamService(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Override
    public void start() {
        vertx.eventBus().consumer("getStream", this::findById);
        vertx.eventBus().consumer("saveStream", this::saveStream);
    }

    private void findById(Message<String> message) {
        mongoClient.findOne("streams", new JsonObject().put("stream_id", message.body()), null,
                result -> message.reply(result.result()));
    }

    private void saveStream(Message<JsonObject> message) {
        mongoClient.insert("streams", message.body(), result -> message.reply(result.result()));
    }
}
