package io;

import org.fastfilter.cuckoo.Cuckoo8;
import org.fastfilter.utils.Hash;
import runner.Config;
import structures.Attribute;
import structures.Candidates;
import structures.Entry;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static java.util.stream.Collectors.*;

public class Validator {
    Config config;
    Attribute[] attributeIndex;
    Candidates candidates;
    Queue<Entry> topValues;
    List<Entry> topGroup;
    BufferedReader[] readers;
    String currentValue;

    public Validator(Config config, Attribute[] attributeIndex, Candidates candidates) throws IOException {
        this.config = config;
        this.attributeIndex = attributeIndex;
        this.candidates = candidates;
        topValues = new PriorityQueue<>();
        topGroup = new ArrayList<>();
        initReaders(attributeIndex);
    }

    public void validate(int layer, Cuckoo8 filter) {

        candidates.calculateViolations(attributeIndex);

        candidates.pruneNull(attributeIndex);

        if (layer > 1) {
            candidates.pruneGlobalUnique(attributeIndex);
        }

        Map<Integer, Long> valueGroup = loadNextGroup();

        long unique = 0;
        long nonUnique = 0;
        while (valueGroup != null) {
            if (layer == 1 && valueGroup.size() > 1) {
                unique++;
                long hash = currentValue.hashCode();
                filter.insert(hash);
            } else {
                nonUnique++;
            }
            candidates.prune(valueGroup);
            updateTopValues();
            valueGroup = loadNextGroup();
        }
        System.out.println(unique + " | " + nonUnique);
        closeReaders();
    }

    private void closeReaders() {
        for (BufferedReader reader : readers) {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * updates every reader that was used in last value group
     */
    private void updateTopValues() {
        // load the new values in parallel
        topGroup.forEach(x -> updateReader(x.getReaderNumber()));

        // clear the current top values
        topGroup.clear();
    }

    /**
     * will load the next value group using the topValue pointers
     *
     * @return A Map of all included attribute id's with their occurrences
     */
    private Map<Integer, Long> loadNextGroup() {

        // if the topValues are empty, the validation is complete
        if (topValues.isEmpty()) return null;

        // poll the first entry. This entry will be used to expand the topValue group
        Entry firstEntry = topValues.poll();
        topGroup.add(firstEntry);

        // add all readers which have the same top value
        while (topValues.peek() != null && topValues.peek().equals(firstEntry)) {
            Entry partOfValueGroup = topValues.poll();
            topGroup.add(partOfValueGroup);
        }

        topGroup.parallelStream().forEach(Entry::load);
        HashMap<Integer, Long> valueGroup = new HashMap<>();
        for (Entry e : topGroup) {
            valueGroup.putAll(e.getConnectedAttributes());
        }

        // set the current value
        currentValue = firstEntry.getValue();
        return valueGroup;
    }

    private void initReaders(Attribute[] attributeIndex) throws IOException {
        List<Integer> relations = Arrays.stream(attributeIndex).mapToInt(Attribute::getRelationId).distinct().boxed().toList();
        readers = new BufferedReader[relations.size()];
        for (int i = 0; i < readers.length; i++) {
            readers[i] = new BufferedReader(new FileReader(config.tempFolder + File.separator + "relation_" + relations.get(i) + ".txt"), 65536);
            updateReader(i);
        }
    }

    private void updateReader(int readerNumber) {
        String nextLine;
        try {
            nextLine = readers[readerNumber].readLine();
            if (nextLine == null) return;
            Entry toAdd = new Entry(nextLine, readers[readerNumber].readLine(), readerNumber);
            topValues.add(toAdd);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
