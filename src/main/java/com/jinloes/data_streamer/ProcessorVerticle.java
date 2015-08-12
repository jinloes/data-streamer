package com.jinloes.data_streamer;

import io.vertx.core.eventbus.Message;

/**
 * Created by jinloes on 8/11/15.
 */
public class ProcessorVerticle extends StreamerVerticle {
    protected String next;
    private boolean startSent = false;

    public ProcessorVerticle(String name, Streamer streamer, String next) {
        super(name, streamer);
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
                startSent = true;
            }
            vertx.eventBus().send(next, document);
        }
    }
}
