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
    Map<String, Map<Integer, Long>> values;
    Logger logger;
    int maxMapSize;
    int currentSize;
    int spillCount;
    List<Path> spilledFiles;

    /**
     * To initialize a sorter, only the maximum map size is required. The constructor will initialize the value map and set the currentSize to 0.
     *
     * @param maxMapSize The maximal summed number of attribute (combinations) to be stored in the nested layer. This value should be as high as possible without risking memory
     *                   overflows for the best possible performance.
     */
    public Sorter(int maxMapSize) {
        this.maxMapSize = maxMapSize;
        values = new HashMap<>();
        currentSize = 0;
        logger = LoggerFactory.getLogger(Sorter.class);
    }


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

                values.computeIfAbsent(value, v -> new HashMap<>());

                if (1 == values.get(value).compute(attribute.getId(), (k, v) -> v == null ? 1 : v + 1)) {
                    currentSize++;
                    if (currentSize > maxMapSize) {
                        spill(sortJob.chunkPath());
                    }
                }
            }
        }
        if (!values.isEmpty()) {
            spill(sortJob.chunkPath());
        }
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
    private void spill(Path chunkPath) {
        spillCount++;
        logger.debug("Spilling " + chunkPath.getFileName() + " # " + spillCount);
        Path spillPath = Path.of(chunkPath + "_" + spillCount + ".txt");
        toDisk(spillPath);
        // keep track of all files that had
        spilledFiles.add(spillPath);
    }

    /**
     * Writes a processed output file
     *
     * @param outputPath The path to which the file is written. Will overwrite an existing file.
     */
    private void toDisk(Path outputPath) {
        try {
            BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);

            for (String value : values.keySet().stream().sorted().toList()) {
                writer.write(value);
                writer.newLine(); // separate the value and the serialized attributes by a new line
                values.get(value).forEach((k, v) -> {
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

        // reset the value map and size count.
        values.clear();
        currentSize = 0;
    }
}
