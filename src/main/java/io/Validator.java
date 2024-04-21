package io;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
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
    List<ValidationReader> readers;

    public Validator(Config config, Candidates candidates, int validationSize) throws IOException {
        this.config = config;
        this.attributeIndex = candidates.current;
        this.candidates = candidates;
        initReaders(validationSize);
        logger = LoggerFactory.getLogger(Validator.class);
    }

    public BloomFilter<Integer> validate(int layer, BloomFilter<Integer> filter) {

        candidates.calculateViolations(attributeIndex);

        candidates.pruneNull(attributeIndex);

        if (layer > 1) {
            candidates.pruneGlobalUnique(attributeIndex);
        }

        if (config.refineFilter) {
            filter = BloomFilter.create(Funnels.integerFunnel(), 100_000_000, 0.05);
        }

        parallelPrune(layer, filter);

        return filter;
    }

    private void parallelPrune(int layer, BloomFilter<Integer> filter) {
        while (!readers.isEmpty()) {
            String maxValue = updateReaders(); // parallel
            HashMap<String, StringBuilder> valueGroupMap = new HashMap<>();

            // single
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

            // parallel
            valueGroupMap.entrySet().stream().parallel().map(group -> {
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
                    if (layer == 1) {
                        return new ValidationTuple(valueGroup, new int[]{group.getKey().hashCode()});
                    }
                    else if (config.refineFilter) {
                        String raw = group.getKey();
                        int[] hashes = new int[layer];
                        int lengthEnc = raw.indexOf('|')+1;
                        String[] lengths = raw.substring(0, lengthEnc-1).split(":");
                        assert lengths.length == layer -1;
                        for (int i = 0; i < layer - 1; i++) {
                            int valueLength = Integer.parseInt(lengths[i]);
                            String s = raw.substring(lengthEnc, lengthEnc+valueLength);
                            hashes[i] = s.hashCode();
                            lengthEnc += valueLength;
                        }
                        String s = raw.substring(lengthEnc);
                        hashes[layer -1] = s.hashCode();

                        return new ValidationTuple(valueGroup, hashes);
                    }
                    else {
                        return new ValidationTuple(valueGroup, null);
                    }
                }
            }).filter(Objects::nonNull).forEach(validationTuple -> {
                if (layer == 1 || config.refineFilter) {
                    synchronized (filter) {
                        for (int hash : validationTuple.hashes()) {
                            filter.put(hash);
                        }
                    }
                }
                candidates.prune(validationTuple.attributeGroup());
            });
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

    private void initReaders(int validationSize) throws IOException {
        List<Integer> relations = Arrays.stream(attributeIndex).mapToInt(Attribute::getRelationId).distinct().boxed().toList();
        readers = new ArrayList<>();
        for (int relation : relations) {
            readers.add(new ValidationReader(config.tempFolder + File.separator + "relation_" + relation + ".txt", validationSize));
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
