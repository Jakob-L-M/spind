package io;

import org.fastfilter.cuckoo.Cuckoo8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.Attribute;
import structures.MergeJob;
import structures.SortJob;
import structures.SortResult;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The sorter is responsible for the creation of one pre-processed file per table. The files consist of as many lines as
 * there are unique values in the whole relational input. The lines are ordered lexicographically by the value which
 * they are associated with. Further each line carries information in which attribute (Combinations) the value is
 * present and also how often it is present in these.
 */
public class Sorter {
    private final int maxMapSize;
    private final long minKeepCount;
    HashMap<String, HashMap<Integer, Long>> values;
    Logger logger;
    int currentSize;
    int spillCount;
    List<Path> spilledFiles;

    /**
     * To initialize a sorter, only the maximum map size is required. The constructor will initialize the value map and set the currentSize to 0.
     *
     * @param maxMapSize The maximal summed number of attribute (combinations) to be stored in the nested layer. This value should be as high as possible without risking memory
     *                   overflows for the best possible performance.
     */
    public Sorter(int maxMapSize, long minKeepCount) {
        this.maxMapSize = maxMapSize;
        this.minKeepCount = minKeepCount;
        values = new HashMap<>();
        currentSize = 0;
        logger = LoggerFactory.getLogger(Sorter.class);
    }

    /**
     * This method processes a sort job. It will first deduplicate the values of a given chunk using a HashMap, while keeping track of with attributes are connected to which
     * value. If the HashMap surpasses the maxMapSize or the end of the input is reached, the keys of the map are sorted and a file will be written to disk. The File has the
     * structure:
     * [Value1]
     * [Serialized Attributes of Value1]
     * [Value2]
     * ....
     *
     * @param sortJob carries information regarding the input path, the connected attributes and the relation, that the chunk is associated with.
     * @param config  carries information on how to parse the chunk file correctly.
     * @param filter  If the layer is at least two, the filter is used to disregard "non-informational" values.
     * @param layer   The current layer, equal to the dimension of the connected attributes.
     * @return A Tuple including a MergeJob and the connected attributes.
     */
    public SortResult process(SortJob sortJob, Config config, Cuckoo8 filter, int layer) {
        spillCount = 0;
        spilledFiles = new ArrayList<>();

        RelationalInput input;
        try {
            input = new RelationalInput(sortJob, config);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        while (input.hasNext()) {
            input.updateAttributeCombinations(filter, layer);
            for (Attribute attribute : input.attributes) {
                String value = attribute.getCurrentValue();

                if (value == null) {
                    attribute.getMetadata().nullEntries++;
                    continue;
                }

                Map<Integer, Long> valueMap = values.computeIfAbsent(value, v -> new HashMap<>());

                if (1L == valueMap.compute(attribute.getId(), (k, v) -> v == null ? 1L : ++v)) {
                    if (++currentSize > maxMapSize) {
                        spill(sortJob.chunkPath(), false);
                    }
                }
            }
        }
        // if there are value which have not been written yet, we need to save them before ending the job
        if (!values.isEmpty()) {
            spill(sortJob.chunkPath(), true);
        }
        // close the input reader
        try {
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new SortResult(new MergeJob(spilledFiles, sortJob.relationId(), null, false), input.attributes);
    }

    /**
     * Will spill the current state to disk and clean the used memory
     *
     * @param chunkPath the path to which the file should be written.
     */
    private void spill(Path chunkPath, boolean isFinal) {
        spillCount++;
        Path spillPath = Path.of(chunkPath + "_" + spillCount + ".txt");
        toDisk(spillPath, isFinal);
        // keep track of all files that had
        spilledFiles.add(spillPath);
    }

    /**
     * Writes a processed output file
     *
     * @param outputPath The path to which the file is written. Will overwrite an existing file.
     */
    private void toDisk(Path outputPath, boolean isFinal) {
        try {
            BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);

            List<Map.Entry<String, HashMap<Integer, Long>>> entriesToSpill;
            if (!isFinal) {
                entriesToSpill = values.entrySet().stream()
                        // only spill values where the occurrences are smaller than some threshold
                        .filter(entry -> entry.getValue().values().stream().reduce(0L, Long::sum) < minKeepCount)
                        .sorted(Map.Entry.comparingByKey()).toList();

                // clean and adjust the values and tracked nested size
                entriesToSpill.forEach(entry -> values.remove(entry.getKey()));
                currentSize -= entriesToSpill.stream().mapToInt(entry -> entry.getValue().size()).sum();

            } else {
                entriesToSpill = values.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList();
                // no need to clean values since the garbage collector will remove the whole class anyway.
            }

            if (isFinal) {
                logger.debug("Spilling " + entriesToSpill.size() + " values to " + outputPath);
            } else {
                logger.debug("Spilling " + entriesToSpill.size() + " values to " + outputPath + ". Keeping " + currentSize + " connected attributes.");
            }

            for (Map.Entry<String, HashMap<Integer, Long>> entry : entriesToSpill) {
                writer.write(entry.getKey());
                writer.newLine(); // separate the value and the serialized attributes by a new line
                entry.getValue().forEach((k, v) -> {
                    try {
                        writer.write(String.valueOf(k));
                        writer.write(','); // attribute-occurrence separator
                        writer.write(String.valueOf(v));
                        writer.write(';'); // attribute-attribute separator
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                writer.newLine();
            }
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
