package com.jinloes.data_streamer;

import io.vertx.core.eventbus.Message;

/**
 * Created by rr2re on 8/7/2015.
 */
public class Reader extends StreamerVerticle {
    private final ReaderStreamer streamer;
    private boolean firstSent = false;

    public Reader(String name, ReaderStreamer streamer, String next) {
        super(name, streamer, next);
        this.streamer = streamer;
    }

    @Override
    public void handleMessage(Message<Document> message) {
        while (streamer.hasNext()) {
            if (!firstSent) {
                vertx.eventBus().send(next, Document.startDocument());
                firstSent = true;
            }
            vertx.eventBus().send(next, streamer.next());
        }
        vertx.eventBus().send(next, Document.endDocument());
    }
}
