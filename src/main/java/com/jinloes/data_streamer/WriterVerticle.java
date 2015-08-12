package com.jinloes.data_streamer;

import io.vertx.core.eventbus.Message;

/**
 * Created by jinloes on 8/11/15.
 */
public class WriterVerticle extends StreamerVerticle {

    public WriterVerticle(String name, Streamer streamer) {
        super(name, streamer);
    }

    @Override
    public void handleMessage(Message<Document> message) {
        streamer.execute(message.body());
    }
}
