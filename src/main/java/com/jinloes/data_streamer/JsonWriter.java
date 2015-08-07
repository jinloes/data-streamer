package com.jinloes.data_streamer;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;

import java.io.File;
import java.io.IOException;

/**
 * Created by rr2re on 8/7/2015.
 */
public class JsonWriter extends AbstractVerticle {
    private JsonFactory jsonFactory = new JsonFactory();
    private JsonGenerator jsonGenerator;

    @Override
    public void start() {
        vertx.eventBus().consumer("writeJson", this::writeJson);
        try {
            jsonGenerator = jsonFactory.createGenerator(new File("C:\\Windows\\Temp\\test.json"),
                    JsonEncoding.UTF8);
        } catch (IOException e) {
            e.printStackTrace();
        }
        jsonGenerator.setCodec(new ObjectMapper());
    }

    private void writeJson(Message<Document> message) {
        Document document = message.body();
        try {
            switch (document.getType()) {
                case START:
                    jsonGenerator.writeStartArray();
                    break;
                case END:
                    jsonGenerator.writeEndArray();
                    jsonGenerator.flush();
                    jsonGenerator.close();
                    break;
                case DATA:
                    jsonGenerator.writeObject(document.getData());
                    break;

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
