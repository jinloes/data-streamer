package com.jinloes.data_streamer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;

/**
 * Created by rr2re on 8/7/2015.
 */
public class StreamerVerticle extends AbstractVerticle {
    private String name;
    protected Streamer streamer;
    protected String next;
    private boolean startSent = false;

    public StreamerVerticle(String name, Streamer streamer, String next) {
        this.name = name;
        this.streamer = streamer;
        this.next = next;
    }

    @Override
    public void start() {
        vertx.eventBus().consumer(name, this::handleMessage);
    }

    public void handleMessage(Message<Document> message) {
        Document document = streamer.execute(message.body());
        if (document != null) {
            if (!startSent) {
                vertx.eventBus().send(next, Document.startDocument());
            }
            vertx.eventBus().send(next, document);
        }
    }
}
