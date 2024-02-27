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
    Logger logger;
    Output output;
    Clock clock;
    RelationMetadata[] relationMetadata;
    int mergeSize = 25;
    int sortSize = 300_000;
    int chunkSize = 500_000;

    public Spind(Config config) {
        this.config = config;
        this.output = new Output(config.resultFolder);
        this.clock = new Clock();
        this.logger = LoggerFactory.getLogger(Spind.class);
        clock.start("total");
    }

    public void execute() throws IOException {

        clock.start("init");
        this.relationMetadata = initializeRelations(chunkSize);

        // 1) init the helper Classes
        Candidates candidates = new Candidates();

        // 1) get all unary attributes.
        Attribute[] attributes = buildUnaryAttributes();
        candidates.loadUnary(attributes);

        logger.info("Finished initialization. Took: " + clock.stop("init") + "ms");

        // 3) while attributes not empty.
        while (attributes.length > 0) {
            candidates.layer++;

            // create sort jobs
            attachAttributes(attributes);
            List<SortJob> sortJobs = createSortJobs();

            logger.info("Starting layer: " + candidates.layer + " with " + attributes.length + " attributes forming " + candidates.current.keySet().stream().mapToInt(x -> candidates.current.get(x).size()).sum() + " candidates");
            // 3.1) Load all attributes of the candidates.
            clock.start("sorting");
            List<MergeJob> mergeJobs = sortJobs.parallelStream().map(sortJob -> {
                        Sorter sorter = new Sorter(sortSize);
                        return sorter.process(sortJob, config);
                    }
            ).toList();
            logger.info("Finished sorting. Took: " + clock.stop("sorting") + "ms");

            clock.start("merging");
            while (!mergeJobs.isEmpty()) {
                // 1) group by relation
                List<MergeJob> groupedMergeJobs = new ArrayList<>();
                for (int i = 0; i < relationMetadata.length; i++) {
                    groupedMergeJobs.add(new MergeJob(new ArrayList<>(), i, null));
                }
                for (MergeJob mergeJob : mergeJobs) {
                    groupedMergeJobs.get(mergeJob.relationId()).chunkPaths().addAll(mergeJob.chunkPaths());
                }

                // split jobs and save next parts
                List<MergeJob> nextJobs = new ArrayList<>();
                List<MergeJob> currentJobs = new ArrayList<>();

                for (MergeJob job : groupedMergeJobs) {
                    if (job.chunkPaths().isEmpty()) {
                        continue;
                    }
                    int n = job.chunkPaths().size();
                    if (n >= mergeSize) {
                        List<Path> nextPaths = new ArrayList<>();
                        for (int i = 0; i < n; i += mergeSize) {
                            Path resultPath = Path.of(job.chunkPaths().get(i) + "_m_" + i + ".txt");
                            currentJobs.add(new MergeJob(new ArrayList<>(job.chunkPaths().subList(i, Math.min(i+ mergeSize,n))), job.relationId(), resultPath));
                            nextPaths.add(resultPath);
                        }
                        nextJobs.add(new MergeJob(nextPaths, job.relationId(), null));
                    }
                    else {
                        Path resultPath = Path.of(config.tempFolder + File.separator + "relation_" + job.relationId() + ".txt");
                        currentJobs.add(new MergeJob(job.chunkPaths(), job.relationId(), resultPath));
                    }
                }

                Attribute[] finalAttributes = attributes;
                currentJobs.parallelStream().forEach(mergeJob -> {
                    if (mergeJob.chunkPaths().isEmpty()) {
                        return;
                    }
                    Merger merger = new Merger();
                    merger.merge(mergeJob.chunkPaths(), mergeJob.to(), finalAttributes);
                });
                mergeJobs = nextJobs;
            }
            logger.info("Finished merging. Took: " + clock.stop("merging") + "ms");

            // 3.2) Validate candidates.
            clock.start("validation");
            Validator validator = new Validator(config, attributes, candidates);
            validator.validate();

            // remove all dependant candidates, that do not reference any attribute
            candidates.cleanCandidates();
            logger.info("Finished validation. Took: " + clock.stop("validation") + "ms");

            int unary = candidates.current.keySet().stream().mapToInt(x -> candidates.current.get(x).size()).sum();
            logger.info("Found " + unary + " pINDs at level " + candidates.layer);

            // output.storePINDs(candidates, inputs, attributes);
            // 3.3) Clean up files.

            // 3.4) Generate new attributes for next layer.
            clock.start("generateNext");
            attributes = candidates.generateNextLayer(attributes, relationMetadata);
            logger.info("Finished generating next layer. Took: " + clock.stop("generateNext") + "ms");
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

    private RelationMetadata[] initializeRelations(int chunkSize) throws IOException {
        RelationMetadata[] relationMetadata = new RelationMetadata[config.tableNames.length];

        int relationOffset = 0;
        for (int relationId = 0; relationId < config.tableNames.length; relationId++) {
            relationMetadata[relationId] = new RelationMetadata(
                    config.tableNames[relationId],
                    relationId,
                    chunkSize,
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
