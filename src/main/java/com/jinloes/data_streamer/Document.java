package com.jinloes.data_streamer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Map;

/**
 * Created by rr2re on 8/7/2015.
 */
public class Document {
    private final Type type;
    private final Map<String, ? extends Object> data;

    private Document(Map<String, ? extends Object> data, Type type) {
        this.data = data;
        this.type = type;
    }

    public static Document startDocument() {
        return new Document(null, Type.START);
    }

    public static Document endDocument() {
        return new Document(null, Type.END);
    }

    public static Document of(Map<String, ? extends Object> data) {
        return new Document(data, Type.DATA);
    }

    public Map<String, ? extends Object> getData() {
        return data;
    }

    public Type getType() {
        return type;
    }

    public boolean isStartDocument() {
        return Type.START.equals(type);
    }

    public boolean isEndDocument() {
        return Type.END.equals(type);
    }

    public enum Type {
        START,
        END,
        DATA;

        @JsonValue
        public String toString() {
            return this.name().toLowerCase();
        }

        @JsonCreator
        public Type fromString(String str) {
            return valueOf(str.toUpperCase());
        }
    }
}
