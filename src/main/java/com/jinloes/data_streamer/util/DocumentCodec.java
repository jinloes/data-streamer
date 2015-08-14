package com.jinloes.data_streamer.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jinloes.data_streamer.Document;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

import java.io.IOException;

/**
 * Serializes/deserializes documents for transfer over the wire.
 */
public class DocumentCodec implements MessageCodec<Document, Document> {
    private final ObjectMapper objectMapper;

    public DocumentCodec() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void encodeToWire(Buffer buffer, Document document) {
        try {
            buffer.appendBytes(objectMapper.writeValueAsBytes(document));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Document decodeFromWire(int pos, Buffer buffer) {
        try {
            return objectMapper.readValue(buffer.getString(0, buffer.length()), Document.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Document transform(Document document) {
        return document;
    }

    @Override
    public String name() {
        return "DocumentCodec";
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }
}
