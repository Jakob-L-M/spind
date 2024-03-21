package io;

import runner.Config;
import structures.Attribute;
import structures.Clock;
import structures.PINDList;
import structures.RelationMetadata;

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
     * @param layer The current layer. Needed to save the pINDs to layer-based files
     * @throws IOException If something goes wrong during output writing
     */
    public void storePINDs(RelationMetadata[] inputs, Attribute[] attributes, int layer) throws IOException {

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
                int refId = it.next().referencedId;
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
     * @param config
     */
    public void storeMetadata(Config config, Clock clock) {
        System.out.println(clock.stop("total"));
    }
}
