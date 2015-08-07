package com.jinloes.data_streamer;

/**
 * Created by rr2re on 8/7/2015.
 */
public abstract class Streamer {
    abstract String getName();

    public Document execute(Document document) {
        switch (document.getType()) {
            case START:
                onStart();
                return null;
            case END:
                onEnd();
                return null;
            case DATA:
                onData(document);
                break;
        }
        return null;
    }

    public void onStart() {

    }

    public void onEnd() {

    }

    public Document onData(Document document) {
        return null;
    }
}
