package io;

import runner.Config;
import structures.Attribute;
import structures.Candidates;
import structures.Clock;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class Output {

    String resultFolder;

    public Output(String resultFolder) {
        this.resultFolder = resultFolder;
    }

    /**
     * Given the currently validated pINDs, a human-readable file will be created.
     *
     * @param candidates The current Candidate class
     * @param inputs The input files
     * @param attributes The current attribute index
     * @throws IOException If something goes wrong during output writing
     */
    public void storePINDs(Candidates candidates, List<RelationalInput> inputs, Attribute[] attributes) throws IOException {
        if (candidates.current.isEmpty()) return;

        BufferedWriter outputWriter = Files.newBufferedWriter(Path.of(this.resultFolder + File.separator + candidates.layer + "-ary_pINDs.txt"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
        for (int depId : candidates.current.keySet()) {
            Attribute dependantAttribute = attributes[depId];
            outputWriter.write('(');
            for (int i = 0; i < dependantAttribute.getContainedColumns().length; i++) {
                RelationalInput input = inputs.get(dependantAttribute.getRelationId());
                outputWriter.write(input.relationName);
                outputWriter.write('.');
                outputWriter.write(input.headerLine[dependantAttribute.getContainedColumns()[i]]);
                if (i != dependantAttribute.getContainedColumns().length - 1) {
                    outputWriter.write(',');
                }
            }
            outputWriter.write(") <=");
            for (int refId : candidates.current.get(depId).keySet()) {
                Attribute referredAttribute = attributes[refId];
                outputWriter.write(" (");
                for (int i = 0; i < referredAttribute.getContainedColumns().length; i++) {
                    RelationalInput input = inputs.get(referredAttribute.getRelationId());
                    outputWriter.write(input.relationName);
                    outputWriter.write('.');
                    outputWriter.write(input.headerLine[referredAttribute.getContainedColumns()[i]]);
                    if (i != referredAttribute.getContainedColumns().length - 1) {
                        outputWriter.write(',');
                    }
                }
                outputWriter.write(')');
            }
            outputWriter.newLine();
        }
        outputWriter.flush();
        outputWriter.close();
    }

    /**
     *
     * @param config
     */
    public void storeMetadata(Config config, Clock clock) {
        System.out.println(clock.stop("total"));
    }
}
