package io;

import structures.Attribute;
import structures.Entry;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

public class Merger {
    private PriorityQueue<Entry> headValues;
    private BufferedReader[] readers;

    private void init(List<Path> files) throws IOException {
        this.headValues = new PriorityQueue<>(files.size());
        this.readers = new BufferedReader[files.size()];

        for (int index = 0; index < files.size(); ++index) {
            BufferedReader reader = Files.newBufferedReader(files.get(index));
            this.readers[index] = reader;
            String firstLine = reader.readLine();
            if (firstLine != null) {
                this.headValues.add(new Entry(firstLine, index));
            }
        }

    }

    public void merge(List<Path> files, Path to, Attribute[] attributes) throws IOException {
        this.init(files);
        BufferedWriter output = Files.newBufferedWriter(to, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        Entry previousValue = null;
        HashMap<Integer, Long> containedAttributes = new HashMap<>();

        while (!this.headValues.isEmpty()) {
            Entry current = this.headValues.poll();
            if (previousValue != null && !previousValue.equals(current)) {
                writeValue(containedAttributes, attributes, output, previousValue.getValue());
                containedAttributes.clear();
            }
            current.getConnectedAttributes().forEach((k, v) -> containedAttributes.merge(k, v, Long::sum));
            previousValue = current;

            updateHeadValues(current);
        }
        writeValue(containedAttributes, attributes, output, previousValue.getValue());
        output.flush();
        output.close();
        closeReaders();
        for (Path path : files) {
            Files.delete(path);
        }
    }

    private void updateHeadValues(Entry current) throws IOException {
        int readerNumber = current.getReaderNumber();
        String nextLine = this.readers[readerNumber].readLine();

        if (nextLine != null) {
            this.headValues.add(new Entry(nextLine, readerNumber));
        }
    }

    private void writeValue(HashMap<Integer, Long> containedAttributes, Attribute[] attributes, BufferedWriter output, String value) throws IOException {
        output.write(value);
        output.write('-'); // delimiter between value and connected attributes
        for (Integer attribute : containedAttributes.keySet()) {
            long occurrences = containedAttributes.get(attribute);

            output.write(String.valueOf(attribute));
            output.write(','); // attribute-occurrence separator
            output.write(String.valueOf(occurrences));
            output.write(';'); // attribute-attribute separator

            attributes[attribute].getMetadata().totalValues += occurrences;
            attributes[attribute].getMetadata().uniqueValues += 1L;
        }
        output.newLine();
    }

    private void closeReaders() throws IOException {
        BufferedReader[] readers = this.readers;

        for (BufferedReader reader : readers) {
            if (reader != null) {
                reader.close();
            }
        }

    }
}