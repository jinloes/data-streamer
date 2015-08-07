package com.jinloes.data_streamer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import org.supercsv.io.CsvMapReader;
import org.supercsv.prefs.CsvPreference;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Created by rr2re on 8/7/2015.
 */
public class CsvReader extends AbstractVerticle {

    @Override
    public void start() {
        vertx.eventBus().consumer("readCsv", this::readCsv);
    }

    private void readCsv(Message<Object> message) {
        vertx.eventBus().send("writeJson", Document.startDocument());
        File file = Paths.get(
                "C:\\Users\\rr2re\\Documents\\workspaces\\data-streamer\\src\\main\\resources\\FL_insurance_sample.csv")
                .toFile();
        try (FileReader reader = new FileReader(file);
             CsvMapReader csvMapReader = new CsvMapReader(reader, CsvPreference.STANDARD_PREFERENCE)) {
            String[] header = csvMapReader.getHeader(true);
            Map<String, String> line;
            while ((line = csvMapReader.read(header)) != null) {
                vertx.eventBus().send("writeJson", Document.of(line));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        vertx.eventBus().send("writeJson", Document.endDocument());
    }
}
