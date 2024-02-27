package io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.Attribute;
import structures.MergeJob;
import structures.SortJob;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

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

    public Sorter(int maxMapSize) {
        this.maxMapSize = maxMapSize;
        values = new HashMap<>();
        currentSize = 0;
        logger = LoggerFactory.getLogger(Sorter.class);
    }


    public MergeJob process(SortJob sortJob, Config config) {
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
            input.updateAttributeCombinations();
            for (Attribute attribute : input.attributes) {
                String value = attribute.getCurrentValue();

                // TODO: add null-handling
                if (value == null) continue;


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
        return new MergeJob(spilledFiles, sortJob.relationId(), null);
    }

    /**
     * Will spill the current state to disk and clean the used memory
     */
    private void spill(Path chunkPath) {
        spillCount++;
        logger.debug("Spilling " + chunkPath.getFileName() + " # " + spillCount);
        Path spillPath = Path.of(chunkPath + "_" + spillCount + ".txt");
        spilledFiles.add(spillPath);
        toDisk(spillPath);
    }

    /**
     * Writes a processed output file
     */
    private void toDisk(Path outputPath) {
        try {
            BufferedWriter bw = Files.newBufferedWriter(outputPath, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);


            for (String value : values.keySet().stream().sorted().toList()) {
                bw.write(value);
                bw.write('-'); // identifier, where the value ends
                Map<Integer, Long> attributesToOccurrences = values.get(value);
                for (Integer attribute : attributesToOccurrences.keySet()) {
                    long occurrences = attributesToOccurrences.get(attribute);

                    bw.write(attribute.toString());
                    bw.write(','); // attribute-occurrence separator
                    bw.write(String.valueOf(occurrences));
                    bw.write(';'); // attribute-attribute separator

                }
                bw.newLine();
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        values = new HashMap<>();
        currentSize = 0;
    }
}
