package com.jinloes.data_streamer;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SystemUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created by rr2re on 8/7/2015.
 */
public class JsonWriter extends Streamer {
    private JsonFactory jsonFactory = new JsonFactory();
    private JsonGenerator jsonGenerator;

    @Override
    String getName() {
        return "JsonWriter";
    }

    @Override
    public void onStart() {
        try {
            String file = SystemUtils.IS_OS_WINDOWS ? "C:\\Windows\\Temp\\test.json" : "/tmp/test.json";
            File f = new File(file);
            if (f.exists()) {
                f.delete();
            }
            f.createNewFile();
            jsonGenerator = jsonFactory.createGenerator(f, JsonEncoding.UTF8);
            jsonGenerator.setCodec(new ObjectMapper());
            jsonGenerator.writeStartArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onEnd() {
        try {
            jsonGenerator.writeEndArray();
            jsonGenerator.flush();
            jsonGenerator.close();
            System.out.println("Done");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Document onData(Document document) {
        try {
            jsonGenerator.writeObject(document.getData());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
