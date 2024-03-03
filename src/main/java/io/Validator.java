package io;

import org.fastfilter.cuckoo.Cuckoo8;
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
    String currentValue;

    public Validator(Config config, Attribute[] attributeIndex, Candidates candidates) throws IOException {
        this.config = config;
        this.attributeIndex = attributeIndex;
        this.candidates = candidates;
        this.readersInTopValues = new ArrayList<>();
        topValues = new PriorityQueue<>();
        initReaders(attributeIndex);
    }

    public void validate(int layer, Cuckoo8 filter) throws IOException {

        if (layer > 1) {
            candidates.pruneGlobalUnique(attributeIndex);
        }

        HashMap<Integer, Long> valueGroup = loadNextGroup();
        long nonUnique = 0;
        long unique = 0;
        while (valueGroup != null) {
            if (valueGroup.size() == 1) {
                unique++;
            } else {
                nonUnique++;
                if (layer == 1) {
                    long hash = currentValue.hashCode();
                    filter.insert(hash);
                }
            }

            candidates.prune(valueGroup);
            updateTopValues();
            valueGroup = loadNextGroup();
        }
        System.out.println("Global unique values: " + unique + ". Global nun-unique values:" + nonUnique);
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
            Entry inGroup = topValues.poll();
            readersInTopValues.add(inGroup.getReaderNumber());
            valueGroup.putAll(inGroup.getConnectedAttributes());
        }
        currentValue = firstEntry.getValue();
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
        Entry toAdd = new Entry(nextLine, readerNumber);
        toAdd.load();
        topValues.add(toAdd);
    }
}
