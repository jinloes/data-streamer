package com.jinloes.data_streamer;

/**
 * Created by rr2re on 8/7/2015.
 */
public abstract class ReaderStreamer extends Streamer {
    abstract boolean hasNext();
    abstract Document next();
}
