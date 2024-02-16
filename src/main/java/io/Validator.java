package io;

import runner.Config;
import structures.Attribute;
import structures.Candidates;
import structures.Entry;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Validator {
    Config config;
    Attribute[] attributeIndex;
    Candidates candidates;
    Queue<Entry> topValues;
    BufferedReader[] readers;
    List<Integer> readersInTopValues;

    public Validator(Config config, Attribute[] attributeIndex, Candidates candidates) throws IOException {
        this.config = config;
        this.attributeIndex = attributeIndex;
        this.candidates = candidates;
        this.readersInTopValues = new ArrayList<>();
        topValues = new PriorityQueue<>();
        initReaders(attributeIndex);
    }

    public void validate() throws IOException {
        HashMap<Integer, Long> valueGroup = loadNextGroup();
        while (valueGroup != null) {
            candidates.prune(valueGroup);
            updateTopValues();
            valueGroup = loadNextGroup();
        }
    }

    private void updateTopValues() throws IOException {
        for (int reader : readersInTopValues) {
            updateReader(reader);
        }
        readersInTopValues.clear();
    }

    public HashMap<Integer, Long> loadNextGroup() {
        if (topValues.isEmpty()) return null;

        Entry firstEntry = topValues.poll();
        readersInTopValues.add(firstEntry.getReaderNumber());
        HashMap<Integer, Long> valueGroup = firstEntry.getConnectedAttributes();
        while (topValues.peek() != null && topValues.peek().equals(firstEntry)) {
            readersInTopValues.add(topValues.peek().getReaderNumber());
            valueGroup.putAll(topValues.poll().getConnectedAttributes()); // can not be null!
        }
        return valueGroup;
    }

    private void initReaders(Attribute[] attributeIndex) throws IOException {
        List<Integer> relations = Arrays.stream(attributeIndex).mapToInt(Attribute::getRelationId).distinct().boxed().toList();
        readers = new BufferedReader[relations.size()];
        for (int i = 0; i < readers.length; i++) {
            readers[i] = Files.newBufferedReader(Path.of(config.tempFolder + File.separator + "relation_" + relations.get(i) + ".txt"));
            updateReader(i);
        }
    }

    private void updateReader(int readerNumber) throws IOException {
        String nextLine = readers[readerNumber].readLine();
        if (nextLine == null) return;
        topValues.add(new Entry(nextLine, readerNumber));
    }
}
