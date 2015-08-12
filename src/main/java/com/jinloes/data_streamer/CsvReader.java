package com.jinloes.data_streamer;

import org.supercsv.io.CsvMapReader;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Created by rr2re on 8/7/2015.
 */
public class CsvReader extends ReaderStreamer {
    private CsvMapReader csvMapReader;
    private String[] header;
    private Map<String, String> next;

    public CsvReader() {
        InputStream file = this.getClass().getClassLoader().getResourceAsStream("FL_insurance_sample.csv");/*Paths.get(
                "C:\\Users\\rr2re\\Documents\\workspaces\\data-streamer\\src\\main\\resources\\FL_insurance_sample.csv")
                .toFile()*/
        ;
        try {
            //FileReader reader = new FileReader(file);
            InputStreamReader reader = new InputStreamReader(file);
            csvMapReader = new CsvMapReader(reader, CsvPreference.STANDARD_PREFERENCE);
            header = csvMapReader.getHeader(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    Document next() {
        Document document = Document.of(next);
        try {
            next = csvMapReader.read(header);
            return document;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    String getName() {
        return "CsvReader";
    }

    @Override
    boolean hasNext() {
        try {
            if (next == null) {
                next = csvMapReader.read(header);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return next != null;
    }
}
