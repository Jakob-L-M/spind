package io;

import runner.Config;
import structures.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class Output {

    String resultFolder;

    public Output(String resultFolder) {
        this.resultFolder = resultFolder;
    }

    /**
     * Given the currently validated pINDs, a human-readable file will be created.
     *
     * @param inputs     The input files
     * @param attributes The current attribute index
     * @param layer      The current layer. Needed to save the pINDs to layer-based files
     * @throws IOException If something goes wrong during output writing
     */
    public void storePINDs(RelationMetadata[] inputs, Attribute[] attributes, int layer, Config config) throws IOException {

        if (!config.writeResults) {
            return;
        }

        BufferedWriter outputWriter = Files.newBufferedWriter(Path.of(this.resultFolder + File.separator + layer + "-ary_pINDs.txt"), StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE);
        for (Attribute dependantAttribute : attributes) {

            if (dependantAttribute.getReferenced() == null) {
                continue;
            }

            outputWriter.write('(');
            for (int i = 0; i < dependantAttribute.getContainedColumns().length; i++) {
                RelationMetadata input = inputs[dependantAttribute.getRelationId()];
                outputWriter.write(input.relationName);
                outputWriter.write('.');
                outputWriter.write(input.columnNames[dependantAttribute.getContainedColumns()[i]]);
                if (i != dependantAttribute.getContainedColumns().length - 1) {
                    outputWriter.write(',');
                }
            }
            outputWriter.write(") <=");
            PINDList.PINDIterator it = dependantAttribute.getReferenced().elementIterator();
            while (it.hasNext()) {
                int refId = it.next().id;
                Attribute referredAttribute = attributes[refId];
                outputWriter.write(" (");
                for (int i = 0; i < referredAttribute.getContainedColumns().length; i++) {
                    RelationMetadata input = inputs[referredAttribute.getRelationId()];
                    outputWriter.write(input.relationName);
                    outputWriter.write('.');
                    outputWriter.write(input.columnNames[referredAttribute.getContainedColumns()[i]]);
                    if (i != referredAttribute.getContainedColumns().length - 1) {
                        outputWriter.write(',');
                    }
                }
                outputWriter.write(')');
            }
            outputWriter.newLine();
        }
        outputWriter.close();
    }

    /**
     *
     * @param config
     * @param clock
     * @param metrics
     * @throws IOException
     */
    public void storeMetadata(Config config, Clock clock, Metrics metrics) throws IOException {
        BufferedWriter outputWriter = Files.newBufferedWriter(Path.of(this.resultFolder + File.separator + config.executionName + "_" + (System.currentTimeMillis() / 1000) + ".json"),
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE);

        outputWriter.write("{\"config\": {" +
                "\"Dataset\":\"" + config.databaseName + '"' +
                ",\"threshold\":" + config.threshold +
                ",\"max n-ary\":" + config.maxNary +
                ",\"CHUNK_SIZE\":" + config.CHUNK_SIZE +
                ",\"SORT_SIZE\":" + config.SORT_SIZE +
                ",\"MERGE_SIZE\":" + config.MERGE_SIZE +
                ",\"VALIDATION_SIZE\":" + config.VALIDATION_SIZE +
                "},");
        outputWriter.write("\"total_time\":" + clock.stop("total"));
        outputWriter.write(",\"files\": {" +
                "\"CHUNK_FILES\":" + metrics.chunkFiles +
                ",\"SORT_FILES\":" + metrics.sortFiles +
                ",\"MERGE_FILES\":" + metrics.mergeFiles +
                "}}");
        outputWriter.close();
    }
}
