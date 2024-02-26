package core;

import io.Merger;
import io.Output;
import io.Sorter;
import io.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Spind {
    Config config;
    int[] relationOffsets;
    Logger logger;
    Output output;
    Clock clock;
    RelationMetadata[] relationMetadata;

    public Spind(Config config) throws IOException {
        this.config = config;
        this.output = new Output(config.resultFolder);
        this.clock = new Clock();
        this.logger = LoggerFactory.getLogger(Spind.class);
        clock.start("total");
        this.relationMetadata = initializeRelations();
    }

    public void execute() throws IOException {

        // 1) init the helper Classes
        Candidates candidates = new Candidates();

        // 1) get all unary attributes.
        Attribute[] attributes = buildUnaryAttributes();
        candidates.loadUnary(attributes);

        // 3) while attributes not empty.
        while (attributes.length > 0) {
            candidates.layer++;

            // create sort jobs
            attachAttributes(attributes);
            List<SortJob> sortJobs = createSortJobs();


            logger.info(" Starting layer: " + candidates.layer + " with " + attributes.length + " attributes forming " + candidates.current.keySet().stream().mapToInt(x -> candidates.current.get(x).size()).sum());
            // 3.1) Load all attributes of the candidates.
            List<MergeJob> mergeJobs = sortJobs.parallelStream().map(sortJob -> {
                        Sorter sorter = new Sorter(200_000);
                        return sorter.process(sortJob, config);
                    }
            ).toList();

            List<MergeJob> groupedMergeJobs = new ArrayList<>();
            for (int i = 0; i < relationMetadata.length; i++) {
                groupedMergeJobs.add(new MergeJob(new ArrayList<>(), i, false));
            }

            for (MergeJob mergeJob : mergeJobs) {
                groupedMergeJobs.get(mergeJob.relationId()).chunkPaths().addAll(mergeJob.chunkPaths());
            }


            Attribute[] finalAttributes = attributes;
            groupedMergeJobs.parallelStream().forEach(mergeJob -> {
                if (mergeJob.chunkPaths().isEmpty()) {
                    return;
                }
                Merger merger = new Merger();
                Path resultPath = Path.of(config.tempFolder + File.separator + "relation_" + mergeJob.relationId() + ".txt");
                merger.merge(mergeJob.chunkPaths(), resultPath, finalAttributes);
            });


            // 3.2) Validate candidates.
            Validator validator = new Validator(config, attributes, candidates);
            validator.validate();

            // remove all dependant candidates, that do not reference any attribute
            candidates.cleanCandidates();

            int unary = candidates.current.keySet().stream().mapToInt(x -> candidates.current.get(x).size()).sum();
            logger.info("Found " + unary + " pINDs at level " + candidates.layer);

            // output.storePINDs(candidates, inputs, attributes);
            // 3.3) Clean up files.

            // 3.4) Generate new attributes for next layer.
            attributes = candidates.generateNextLayer(attributes, relationMetadata);
        }

        // clean chunks
        for (RelationMetadata relation : relationMetadata) {
            for (Path chunk : relation.chunks) {
                Files.delete(chunk);
            }
        }

        // 4) Save the output
        output.storeMetadata(config, clock);
    }

    private RelationMetadata[] initializeRelations() throws IOException {
        RelationMetadata[] relationMetadata = new RelationMetadata[config.tableNames.length];

        int relationOffset = 0;
        for (int relationId = 0; relationId < config.tableNames.length; relationId++) {
            relationMetadata[relationId] = new RelationMetadata(
                    config.tableNames[relationId],
                    relationId,
                    250_000,
                    relationOffset,
                    Path.of(config.folderPath + File.separator + config.databaseName + File.separator + config.tableNames[relationId] + config.fileEnding),
                    config
            );
            relationOffset += relationMetadata[relationId].columnNames.length;
        }
        return relationMetadata;
    }

    private Attribute[] buildUnaryAttributes() {
        Attribute[] attributes = new Attribute[Arrays.stream(relationMetadata).mapToInt(x -> x.columnNames.length).sum()];
        for (RelationMetadata input : relationMetadata) {
            int relationOffset = input.relationOffset;
            for (int i = 0; i < input.columnNames.length; i++) {
                attributes[relationOffset + i] = new Attribute(
                        relationOffset + i,
                        input.relationId,
                        new int[]{i}
                );
            }
        }
        return attributes;
    }

    private void attachAttributes(Attribute[] attributes) {
        // reset connectedAttributes
        for (RelationMetadata relation : relationMetadata) {
            relation.connectedAttributes = new ArrayList<>();
        }
        for (Attribute attribute : attributes) {
            relationMetadata[attribute.getRelationId()].connectedAttributes.add(attribute);
        }
        for (RelationMetadata relation : relationMetadata) {
            relation.connectedAttributes = Collections.unmodifiableList(relation.connectedAttributes);
        }
    }

    private List<SortJob> createSortJobs() {
        List<SortJob> jobs = new ArrayList<>();

        for (RelationMetadata relation : relationMetadata) {
            if (relation.connectedAttributes.isEmpty()) {
                continue;
            }
            for (Path chunkPath : relation.chunks) {
                jobs.add(new SortJob(chunkPath, relation.connectedAttributes, relation.relationId));
            }
        }

        return jobs;
    }
}
