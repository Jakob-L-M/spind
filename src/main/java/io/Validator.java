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
    List<Integer> valueGroupReaders;
    String currentValue;

    public Validator(Config config, Attribute[] attributeIndex, Candidates candidates) throws IOException {
        this.config = config;
        this.attributeIndex = attributeIndex;
        this.candidates = candidates;
        this.valueGroupReaders = new ArrayList<>();
        topValues = new PriorityQueue<>();
        initReaders(attributeIndex);
    }

    public void validate(int layer, Cuckoo8 filter) {

        if (layer > 1) {
            candidates.pruneGlobalUnique(attributeIndex);
        }

        HashMap<Integer, Long> valueGroup = loadNextGroup();

        while (valueGroup != null) {
            if (layer == 1 && valueGroup.size() > 1) {
                long hash = currentValue.hashCode();
                filter.insert(hash);
            }
            candidates.prune(valueGroup);
            updateTopValues();
            valueGroup = loadNextGroup();
        }
    }

    /**
     * updates every reader that was used in last value group
     */
    private void updateTopValues() {
        // load the new values in parallel
        valueGroupReaders.forEach(this::updateReader);

        // clear the current top values
        valueGroupReaders.clear();
    }

    /**
     * will load the next value group using the topValue pointers
     *
     * @return A Map of all included attribute id's with their occurrences
     */
    private HashMap<Integer, Long> loadNextGroup() {

        // if the topValues are empty, the validation is complete
        if (topValues.isEmpty()) return null;

        // poll the first entry. This entry will be used to expand the topValue group
        Entry firstEntry = topValues.poll();
        valueGroupReaders.add(firstEntry.getReaderNumber());
        HashMap<Integer, Long> valueGroup = firstEntry.getConnectedAttributes();

        // add all readers which have the same top value
        while (topValues.peek() != null && topValues.peek().equals(firstEntry)) {
            Entry partOfValueGroup = topValues.poll();
            valueGroupReaders.add(partOfValueGroup.getReaderNumber());
            valueGroup.putAll(partOfValueGroup.getConnectedAttributes());
        }

        // set the current value
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

    private void updateReader(int readerNumber) {
        String nextLine;
        try {
            nextLine = readers[readerNumber].readLine();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        if (nextLine == null) return;
        Entry toAdd = new Entry(nextLine, readerNumber);
        toAdd.load();
        topValues.add(toAdd);
    }
}
