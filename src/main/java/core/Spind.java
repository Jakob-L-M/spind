package core;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import io.Merger;
import io.Output;
import io.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @noinspection UnstableApiUsage
 */
public class Spind {
    final int CHUNK_SIZE;
    final int SORT_SIZE;
    final int MERGE_SIZE;
    final int VALIDATION_SIZE;
    final Config config;
    private final Metrics metrics;
    private final int maxNary;
    private final Logger logger;
    private final Output output;
    private final Clock clock;
    RelationMetadata[] relationMetadata;
    int layer;
    private BloomFilter<Integer> filter;

    public Spind(Config config) {
        this.clock = new Clock();
        clock.start("total");

        this.metrics = new Metrics();
        this.config = config;
        maxNary = config.maxNary;
        this.output = new Output(config.resultFolder);
        this.logger = LoggerFactory.getLogger(Spind.class);
        if (config.useFilter) this.filter = BloomFilter.create(Funnels.integerFunnel(), 100_000_000, 0.05);

        CHUNK_SIZE = config.CHUNK_SIZE;
        SORT_SIZE = config.SORT_SIZE;
        MERGE_SIZE = config.MERGE_SIZE;
        VALIDATION_SIZE = config.VALIDATION_SIZE;
    }

    public void execute() throws IOException, InterruptedException {

        logger.info("Starting execution");

        clock.start("init");
        this.relationMetadata = initializeRelations();

        // 1) init the helper Classes
        Candidates candidates = new Candidates(config);

        // 1) get all unary attributes.
        Attribute[] attributes = buildUnaryAttributes();
        candidates.loadUnary(attributes);

        logger.info("Finished initialization. Took: " + clock.stop("init") + "ms");

        // 3) while attributes not empty.
        while (attributes.length > 0) {
            layer++;

            // create sort jobs
            attachAttributes(attributes);
            List<SortJob> sortJobs = createSortJobs();

            int numAttributes = attributes.length;
            int numCandidates = Arrays.stream(attributes).mapToInt(x -> x.getReferenced() == null ? 0 : x.getReferenced().size()).sum();

            metrics.layerAttributes.add(numAttributes);
            metrics.layerCandidates.add(numCandidates);
            logger.info("Starting layer: " + layer + " with " + numAttributes + " attributes forming " + numCandidates + " candidates");
            // candidates.current.get(x).size()).sum() + " candidates");
            // 3.1) Load all attributes of the candidates.
            clock.start("sorting");
            ExecutorService executors = Executors.newFixedThreadPool(config.PARALLEL);
            List<SortResult> sortResults = executors.invokeAll(sortJobs.stream().sorted().toList()).stream().map(sortResultFuture -> {
                try {
                    return sortResultFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    return new SortResult(null, null);
                }
            }).toList();
            executors.shutdown();
            /*
            List<SortResult> sortResults = sortJobs.parallelStream().map(sortJob -> {
                        logger.debug("Starting to sort: " + sortJob.chunkPath());
                        Sorter sorter = new Sorter(SORT_SIZE, (long) (sortJob.connectedAttributes().size()) * 10 * CHUNK_SIZE / SORT_SIZE);
                        return sorter.process(sortJob, config, filter, layer);
                    }
            ).toList();
            */
            logger.info("Finished sorting. Took: " + clock.stop("sorting") + "ms");

            metrics.sortFiles += sortResults.stream().mapToInt(sortResult -> sortResult.mergeJob().chunkPaths().size()).sum();

            long totalSaved = 0;
            for (SortResult sortResult : sortResults) {
                for (Attribute sortAttribute : sortResult.connectedAttributes()) {
                    attributes[sortAttribute.getId()].getMetadata().globalUnique += sortAttribute.getMetadata().globalUnique;
                    attributes[sortAttribute.getId()].getMetadata().nullEntries += sortAttribute.getMetadata().nullEntries;
                    totalSaved += sortAttribute.getMetadata().globalUnique;
                }
            }
            logger.info("In total " + totalSaved + " occurrences where skipped due to global uniqueness");

            List<MergeJob> mergeJobs = sortResults.stream().map(SortResult::mergeJob).toList();

            clock.start("merging");
            iterativeMerge(attributes, mergeJobs);
            logger.info("Finished merging. Took: " + clock.stop("merging") + "ms");

            // 3.2) Validate candidates.
            clock.start("validation");
            Validator validator = new Validator(config, candidates, VALIDATION_SIZE);
            filter = validator.validate(layer, filter);

            // remove all dependant candidates, that do not reference any attribute
            candidates.cleanCandidates();
            logger.info("Finished validation. Took: " + clock.stop("validation") + "ms");

            int numPINDs = calcPINDs(attributes);
            metrics.layerPINDs.add(numPINDs);
            if (layer == 1) metrics.unary = numPINDs;
            else metrics.nary += numPINDs;

            logger.info("Found " + numPINDs + " pINDs at level " + layer);
            output.storePINDs(relationMetadata, attributes, layer, config);

            // clean relation files
            for (RelationMetadata relation : relationMetadata) {
                Path relationFile = Path.of(config.tempFolder + File.separator + "relation_" + relation.relationId + ".txt");
                if (Files.exists(relationFile)) Files.delete(relationFile);
            }

            if (maxNary > 0 && layer == maxNary) break;

            // 3.4) Generate new attributes for next layer.
            clock.start("generateNext");
            attributes = candidates.generateNextLayer(attributes, relationMetadata, layer);
            logger.info("Finished generating next layer. Took: " + clock.stop("generateNext") + "ms");
        }
        // clean up temp
        Arrays.stream(Objects.requireNonNull((new File(config.tempFolder)).listFiles())).forEach(File::delete);

        // 4) Save the output
        output.storeMetadata(config, clock, metrics);
    }

    private void iterativeMerge(Attribute[] attributes, List<MergeJob> mergeJobs) {
        while (!mergeJobs.isEmpty()) {
            // 1) group by relation
            List<MergeJob> groupedMergeJobs = new ArrayList<>();
            for (int i = 0; i < relationMetadata.length; i++) {
                groupedMergeJobs.add(new MergeJob(new ArrayList<>(), i, null, false));
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
                if (n >= MERGE_SIZE) {
                    // Case 1: The number of files exceeds the merge size threshold -> Merge subsets of the spilled files and re-add then to the next iteration
                    List<Path> nextPaths = new ArrayList<>();
                    for (int i = 0; i < n; i += MERGE_SIZE) {
                        Path resultPath = Path.of(job.chunkPaths().get(i) + "_m_" + i + ".txt");
                        currentJobs.add(new MergeJob(new ArrayList<>(job.chunkPaths().subList(i, Math.min(i + MERGE_SIZE, n))), job.relationId(), resultPath, false));
                        nextPaths.add(resultPath);
                    }
                    nextJobs.add(new MergeJob(nextPaths, job.relationId(), null, false));
                } else {
                    // Case 2: The number of files does not exceed the threshold -> the next merge finishes the relation file.
                    Path resultPath = Path.of(config.tempFolder + File.separator + "relation_" + job.relationId() + ".txt");
                    currentJobs.add(new MergeJob(job.chunkPaths(), job.relationId(), resultPath, true));
                }
            }

            metrics.mergeFiles += currentJobs.size();

            currentJobs.parallelStream().forEach(mergeJob -> {
                if (mergeJob.chunkPaths().isEmpty()) {
                    return;
                }
                Merger merger = new Merger();
                merger.merge(mergeJob.chunkPaths(), mergeJob.to(), attributes, mergeJob.isFinal());
            });
            mergeJobs = nextJobs;
        }
    }

    private int calcPINDs(Attribute[] attributes) {
        int total = 0;
        for (Attribute attribute : attributes) {
            if (attribute.getReferenced() != null) {
                total += attribute.getReferenced().size();
            }
        }
        return total;
    }

    private RelationMetadata[] initializeRelations() throws IOException {
        RelationMetadata[] relationMetadata = new RelationMetadata[config.tableNames.length];

        int relationOffset = 0;
        for (int relationId = 0; relationId < config.tableNames.length; relationId++) {
            relationMetadata[relationId] = new RelationMetadata(config.tableNames[relationId], relationId, relationOffset,
                    Path.of(config.folderPath + File.separator + config.databaseName + File.separator + config.tableNames[relationId] + config.fileEnding), config);
            relationOffset += relationMetadata[relationId].columnNames.length;
        }

        clock.start("chunking");
        logger.info("Stating chunking");
        Arrays.stream(relationMetadata).parallel().forEach(relation -> {
            long sTime = System.currentTimeMillis();
            try {
                relation.createChunks(CHUNK_SIZE / relation.columnNames.length, config);
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.debug("Finished " + relation.relationName + " (" + (System.currentTimeMillis() - sTime) + "ms)");
        });
        logger.info("Finished chunking. Took: " + clock.stop("chunking"));

        this.metrics.chunkFiles = Arrays.stream(relationMetadata).mapToInt(metadata -> metadata.chunks.size()).sum();

        return relationMetadata;
    }

    private Attribute[] buildUnaryAttributes() {
        Attribute[] attributes = new Attribute[Arrays.stream(relationMetadata).mapToInt(x -> x.columnNames.length).sum()];
        for (RelationMetadata input : relationMetadata) {
            int relationOffset = input.relationOffset;
            for (int i = 0; i < input.columnNames.length; i++) {
                attributes[relationOffset + i] = new Attribute(relationOffset + i, input.relationId, new int[]{i});
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
                jobs.add(new SortJob(chunkPath, relation.connectedAttributes, relation.relationId, config.SORT_SIZE, config.CHUNK_SIZE, config, filter, layer));
            }
        }

        return jobs;
    }
}
