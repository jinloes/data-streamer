package com.jinloes.data_streamer;

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
public class CsvReader extends ReaderStreamer {
    private CsvMapReader csvMapReader;
    private String[] header;
    private Map<String, String> next;

    public CsvReader() {
        File file = Paths.get(
                "C:\\Users\\rr2re\\Documents\\workspaces\\data-streamer\\src\\main\\resources\\FL_insurance_sample.csv")
                .toFile();
        try {
            FileReader reader = new FileReader(file);
            csvMapReader = new CsvMapReader(reader, CsvPreference.STANDARD_PREFERENCE);
            header = csvMapReader.getHeader(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    Document next() {
        return Document.of(next);
    }

    @Override
    String getName() {
        return "CsvReader";
    }

    @Override
    boolean hasNext() {
        try {
            next = csvMapReader.read(header);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return next != null;
    }
}
