package com.jinloes.data_streamer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;

/**
 * Created by rr2re on 8/7/2015.
 */
public abstract class StreamerVerticle extends AbstractVerticle {
    protected String name;
    protected Streamer streamer;

    public StreamerVerticle(String name, Streamer streamer) {
        this.name = name;
        this.streamer = streamer;
    }

    @Override
    public void start() {
        vertx.eventBus().consumer(name, this::handleMessage);
    }

    public abstract void handleMessage(Message<Document> message);
}
