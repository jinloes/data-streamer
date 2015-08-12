package com.jinloes.data_streamer;

import io.vertx.core.eventbus.Message;

/**
 * Created by rr2re on 8/7/2015.
 */
public class ReaderVerticle extends StreamerVerticle {
    private final ReaderStreamer streamer;
    private final String next;

    public ReaderVerticle(String name, ReaderStreamer streamer, String next) {
        super(name, streamer);
        this.streamer = streamer;
        this.next = next;
    }

    @Override
    public void handleMessage(Message<Document> message) {
        vertx.eventBus().send(next, Document.startDocument());
        while (streamer.hasNext()) {
            vertx.eventBus().send(next, streamer.next());
        }
        vertx.eventBus().send(next, Document.endDocument());
    }
}
