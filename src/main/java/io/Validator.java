package io;

import org.fastfilter.cuckoo.Cuckoo8;
import org.fastfilter.utils.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class Validator {
    Config config;
    Logger logger;
    Attribute[] attributeIndex;
    Candidates candidates;
    PriorityQueue<Entry> topValues;
    List<Entry> topGroup;
    List<ValidationReader> readers;
    String currentValue;

    public Validator(Config config, Attribute[] attributeIndex, Candidates candidates) throws IOException {
        this.config = config;
        this.attributeIndex = candidates.current;
        this.candidates = candidates;
        initReaders();
        logger = LoggerFactory.getLogger(Validator.class);
    }

    public void validate(int layer, Cuckoo8 filter) {

        candidates.calculateViolations(attributeIndex);

        candidates.pruneNull(attributeIndex);

        if (layer > 1) {
            candidates.pruneGlobalUnique(attributeIndex);
        }

        while (!readers.isEmpty()) {
            String maxValue = updateReaders();
            HashMap<String, StringBuilder> valueGroupMap = new HashMap<>();
            for (ValidationReader reader : readers) {
                Entry next = reader.queue.poll();
                while (next != null && next.getValue().compareTo(maxValue) <= 0) {
                    String connectedAttributes = next.getSerializedAttributes();
                    valueGroupMap.compute(next.getValue(), (k, v) -> v == null ? new StringBuilder(connectedAttributes) : v.append(connectedAttributes));
                    next = reader.queue.poll();
                }
                if (next != null) {
                    // if the most recent value is bigger than the biggest safe value, we re-add it to the front of the queue.
                    reader.queue.addFirst(next);
                }
            }

            List<ValidationTuple> valueGroups = valueGroupMap.entrySet().stream().parallel().map(group -> {
                HashMap<Integer, Long> valueGroup = buildAttributeMap(group.getValue().toString());
                boolean onlyRef = true;
                // TODO: idea -> maybe already remove non-interesting attributes here.
                for (int attributeId : valueGroup.keySet()) {
                    if (attributeIndex[attributeId].getReferenced() != null) {
                        onlyRef = false;
                        break;
                    }
                }
                if (onlyRef) {
                    return null;
                } else {
                    return new ValidationTuple(valueGroup, (long) group.getKey().hashCode());
                }
            }).filter(Objects::nonNull).toList();

            for (ValidationTuple validationTuple : valueGroups) {
                HashMap<Integer, Long> valueGroup = validationTuple.attributeGroup();
                if (layer == 1 && valueGroup.size() > 1) {
                    filter.insert(validationTuple.hash());
                }
                candidates.prune(valueGroup);
            }

            cleanReaders();
        }
    }

    /**
     * updates every reader that was used in last value group
     */
    private String updateReaders() {
        // load the new values in parallel
        return readers.stream().parallel().map(ValidationReader::update).min(String::compareTo).orElse(null);
    }

    private void cleanReaders() {
        Iterator<ValidationReader> it = readers.iterator();
        while (it.hasNext()) {
            ValidationReader next = it.next();
            if (next.finished && next.queue.isEmpty()) {
                next.close();
                it.remove();
            }
        }
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
        Entry nextEntry = topValues.peek();
        while (nextEntry != null && nextEntry.equals(firstEntry)) {
            Entry partOfValueGroup = topValues.poll();
            topGroup.add(partOfValueGroup);
            nextEntry = topValues.peek();
        }

        // Notice: this part takes the longest, by quite a bit.
        topGroup.forEach(Entry::load);
        HashMap<Integer, Long> valueGroup = new HashMap<>();
        for (Entry e : topGroup) {
            valueGroup.putAll(e.getConnectedAttributes());
        }
        // End of notice

        // set the current value
        currentValue = firstEntry.getValue();
        return valueGroup;
    }

    private void initReaders() throws IOException {
        List<Integer> relations = Arrays.stream(attributeIndex).mapToInt(Attribute::getRelationId).distinct().boxed().toList();
        readers = new ArrayList<>();
        for (int relation : relations) {
            readers.add(new ValidationReader(config.tempFolder + File.separator + "relation_" + relation + ".txt", 2 ^ 13, 100));
        }
    }

    private HashMap<Integer, Long> buildAttributeMap(String serializedAttributes) {
        HashMap<Integer, Long> connectedAttributes = new HashMap<>();
        String[] attributes = serializedAttributes.split(";");
        for (String attribute : attributes) {
            String[] idOccurrenceTuple = attribute.split(",");
            if (idOccurrenceTuple.length != 2) {
                continue;
            }
            connectedAttributes.put(Integer.valueOf(idOccurrenceTuple[0]), Long.valueOf(idOccurrenceTuple[1]));
        }
        return connectedAttributes;
    }
}
