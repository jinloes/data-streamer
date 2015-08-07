package com.jinloes.data_streamer;

import io.vertx.core.AbstractVerticle;

/**
 * Created by rr2re on 8/7/2015.
 */
public class Filter extends AbstractVerticle {

    @Override
    public void start() {
        vertx.eventBus().consumer("filter");
    }

    private void filter() {

    }
}
