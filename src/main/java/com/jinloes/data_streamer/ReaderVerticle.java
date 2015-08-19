package com.jinloes.data_streamer;

import io.vertx.core.eventbus.Message;

import java.util.List;

/**
 * Created by rr2re on 8/7/2015.
 */
public class ReaderVerticle extends StreamerVerticle {
    private final ReaderStreamer streamer;
    private final List<String> outputAddresses;

    public ReaderVerticle(String name, ReaderStreamer streamer, List<String> outputAddresses) {
        super(name, streamer);
        this.streamer = streamer;
        this.outputAddresses = outputAddresses;
    }

    @Override
    public void handleMessage(Message<Document> message) {
        for (String address : outputAddresses) {
            vertx.eventBus().send(address, Document.startDocument());
        }
        while (streamer.hasNext()) {
            for (String address : outputAddresses) {
                vertx.eventBus().send(address, streamer.next());
            }
        }
        for (String address : outputAddresses) {
            vertx.eventBus().send(address, Document.endDocument());
        }
    }
}
