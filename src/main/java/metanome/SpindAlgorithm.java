package metanome;

import core.Spind;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_execution.FileGenerator;
import de.metanome.algorithm_integration.algorithm_types.*;
import de.metanome.algorithm_integration.configuration.*;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.RelaxedInclusionDependencyResultReceiver;
import runner.Config;
import structures.RelationMetadata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

/** Metanome entry point for SPIND's unary and n-ary partial inclusion dependencies. */
public class SpindAlgorithm implements RelaxedInclusionDependencyAlgorithm, RelationalInputParameterAlgorithm,
        StringParameterAlgorithm, IntegerParameterAlgorithm, BooleanParameterAlgorithm, TempFileAlgorithm {
    public static final String INPUT_FILES = "INPUT_FILES";
    public static final String THRESHOLD = "THRESHOLD";
    public static final String MAX_ARITY = "MAX_ARITY";
    public static final String PARALLELISM = "PARALLELISM";
    public static final String CHUNK_SIZE = "CHUNK_SIZE";
    public static final String SORT_SIZE = "SORT_SIZE";
    public static final String MERGE_SIZE = "MERGE_SIZE";
    public static final String VALIDATION_SIZE = "VALIDATION_SIZE";
    public static final String NULL_HANDLING = "NULL_HANDLING";
    public static final String DUPLICATE_HANDLING = "DUPLICATE_HANDLING";
    public static final String USE_FILTER = "USE_FILTER";
    public static final String REFINE_FILTER = "REFINE_FILTER";

    private final Config settings = new Config(0.93);
    private double threshold = 0.93;
    private RelationalInputGenerator[] generators;
    private RelaxedInclusionDependencyResultReceiver receiver;
    private FileGenerator tempFileGenerator;

    @Override
    public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
        ArrayList<ConfigurationRequirement<?>> requirements = new ArrayList<>();
        ConfigurationRequirementRelationalInput inputs = new ConfigurationRequirementRelationalInput(INPUT_FILES, 1, -1);
        inputs.setRequired(true);
        requirements.add(inputs);
        Config defaults = new Config(0.93);
        try {
            addString(requirements, THRESHOLD, "0.93");
            addString(requirements, NULL_HANDLING, defaults.nullHandling.name());
            addString(requirements, DUPLICATE_HANDLING, defaults.duplicateHandling.name());
            addInteger(requirements, MAX_ARITY, defaults.maxNary);
            addInteger(requirements, PARALLELISM, defaults.PARALLEL);
            addInteger(requirements, CHUNK_SIZE, defaults.CHUNK_SIZE);
            addInteger(requirements, SORT_SIZE, defaults.SORT_SIZE);
            addInteger(requirements, MERGE_SIZE, defaults.MERGE_SIZE);
            addInteger(requirements, VALIDATION_SIZE, defaults.VALIDATION_SIZE);
            addBoolean(requirements, USE_FILTER, defaults.useFilter);
            addBoolean(requirements, REFINE_FILTER, defaults.refineFilter);
        } catch (AlgorithmConfigurationException e) {
            throw new IllegalStateException("Invalid SPIND defaults", e);
        }
        return requirements;
    }

    private void addString(List<ConfigurationRequirement<?>> requirements, String key, String value)
            throws AlgorithmConfigurationException {
        ConfigurationRequirementString requirement = new ConfigurationRequirementString(key);
        requirement.checkAndSetDefaultValues(value);
        requirements.add(requirement);
    }

    private void addInteger(List<ConfigurationRequirement<?>> requirements, String key, int value)
            throws AlgorithmConfigurationException {
        ConfigurationRequirementInteger requirement = new ConfigurationRequirementInteger(key);
        requirement.checkAndSetDefaultValues(value);
        requirements.add(requirement);
    }

    private void addBoolean(List<ConfigurationRequirement<?>> requirements, String key, boolean value)
            throws AlgorithmConfigurationException {
        ConfigurationRequirementBoolean requirement = new ConfigurationRequirementBoolean(key);
        requirement.checkAndSetDefaultValues(value);
        requirements.add(requirement);
    }

    @Override
    public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values)
            throws AlgorithmConfigurationException {
        if (!INPUT_FILES.equals(identifier) || values == null || values.length == 0
                || Arrays.stream(values).anyMatch(java.util.Objects::isNull)) {
            throw new AlgorithmConfigurationException("INPUT_FILES requires at least one relational input");
        }
        generators = values.clone();
    }

    private <T> T single(String identifier, T[] values) throws AlgorithmConfigurationException {
        if (values == null || values.length != 1 || values[0] == null) {
            throw new AlgorithmConfigurationException(identifier + " requires exactly one non-null value");
        }
        return values[0];
    }

    @Override
    public void setStringConfigurationValue(String identifier, String... values) throws AlgorithmConfigurationException {
        String value = single(identifier, values);
        try {
            switch (identifier) {
                case THRESHOLD -> {
                    double parsed = Double.parseDouble(value);
                    if (!Double.isFinite(parsed) || parsed < 0 || parsed > 1) throw new IllegalArgumentException();
                    threshold = parsed;
                }
                case NULL_HANDLING -> settings.nullHandling = Config.NullHandling.valueOf(value);
                case DUPLICATE_HANDLING -> settings.duplicateHandling = Config.DuplicateHandling.valueOf(value);
                default -> throw new AlgorithmConfigurationException("Unknown string parameter: " + identifier);
            }
        } catch (IllegalArgumentException e) {
            throw new AlgorithmConfigurationException("Invalid " + identifier + ": " + value);
        }
    }

    @Override
    public void setIntegerConfigurationValue(String identifier, Integer... values) throws AlgorithmConfigurationException {
        int value = single(identifier, values);
        if (MAX_ARITY.equals(identifier)) {
            if (value != -1 && value < 1) throw new AlgorithmConfigurationException("MAX_ARITY must be -1 or positive");
            settings.maxNary = value;
            return;
        }
        if (value < 1) throw new AlgorithmConfigurationException(identifier + " must be positive");
        switch (identifier) {
            case PARALLELISM -> settings.PARALLEL = value;
            case CHUNK_SIZE -> settings.CHUNK_SIZE = value;
            case SORT_SIZE -> settings.SORT_SIZE = value;
            case MERGE_SIZE -> {
                if (value < 2) throw new AlgorithmConfigurationException("MERGE_SIZE must be at least 2");
                settings.MERGE_SIZE = value;
            }
            case VALIDATION_SIZE -> settings.VALIDATION_SIZE = value;
            default -> throw new AlgorithmConfigurationException("Unknown integer parameter: " + identifier);
        }
    }

    @Override
    public void setBooleanConfigurationValue(String identifier, Boolean... values) throws AlgorithmConfigurationException {
        boolean value = single(identifier, values);
        switch (identifier) {
            case USE_FILTER -> settings.useFilter = value;
            case REFINE_FILTER -> settings.refineFilter = value;
            default -> throw new AlgorithmConfigurationException("Unknown boolean parameter: " + identifier);
        }
    }

    @Override
    public void setResultReceiver(RelaxedInclusionDependencyResultReceiver receiver) {
        this.receiver = receiver;
    }

    @Override
    public void setTempFileGenerator(FileGenerator generator) {
        tempFileGenerator = generator;
    }

    @Override
    public void execute() throws AlgorithmExecutionException {
        if (generators == null) throw new AlgorithmConfigurationException("INPUT_FILES has not been configured");
        if (receiver == null) throw new AlgorithmConfigurationException("Result receiver has not been configured");
        if (settings.SORT_SIZE < settings.PARALLEL) {
            throw new AlgorithmConfigurationException("SORT_SIZE must be at least PARALLELISM");
        }
        Config config = executionConfig();
        try {
            Path directory = tempFileGenerator == null ? Files.createTempDirectory("spind-")
                    : Files.createTempDirectory(tempFileGenerator.getTemporaryFile().toPath().toAbsolutePath().getParent(), "spind-");
            try (Workspace workspace = new Workspace(directory)) {
                config.tempFolder = directory.toString();
                config.tableNames = new String[generators.length];
                RelationMetadata[] relations = new RelationMetadata[generators.length];
                HashSet<String> names = new HashSet<>();
                int offset = 0;
                for (int i = 0; i < generators.length; i++) {
                    RelationalInput input = generators[i].generateNewCopy();
                    MetanomeInput rows = new MetanomeInput(input);
                    workspace.inputs.add(rows);
                    String name = input.relationName();
                    if (name == null || !names.add(name)) {
                        throw new AlgorithmConfigurationException("Input relation names must be non-null and distinct: " + name);
                    }
                    if (input.numberOfColumns() < 1 || rows.columnNames().length != input.numberOfColumns()) {
                        throw new AlgorithmConfigurationException("Invalid schema for " + name);
                    }
                    config.tableNames[i] = name;
                    relations[i] = new RelationMetadata(i, offset, rows, 0, config);
                    offset += input.numberOfColumns();
                }
                new Spind(config, relations, new MetanomeOutput(receiver)).execute();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AlgorithmExecutionException("SPIND execution interrupted", e);
        } catch (AlgorithmExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new AlgorithmExecutionException("SPIND execution failed", e);
        }
    }

    private Config executionConfig() {
        Config config = new Config(threshold);
        config.maxNary = settings.maxNary;
        config.PARALLEL = settings.PARALLEL;
        config.CHUNK_SIZE = settings.CHUNK_SIZE;
        config.SORT_SIZE = settings.SORT_SIZE;
        config.MERGE_SIZE = settings.MERGE_SIZE;
        config.VALIDATION_SIZE = settings.VALIDATION_SIZE;
        config.nullHandling = settings.nullHandling;
        config.duplicateHandling = settings.duplicateHandling;
        config.useFilter = settings.useFilter;
        config.refineFilter = settings.refineFilter;
        config.writeResults = false;
        config.writeMetadata = false;
        config.encodeChunkValues = true;
        config.databaseName = "Metanome";
        return config;
    }

    private static final class Workspace implements AutoCloseable {
        private final Path directory;
        private final List<MetanomeInput> inputs = new ArrayList<>();

        private Workspace(Path directory) {
            this.directory = directory;
        }

        @Override
        public void close() throws IOException {
            IOException failure = null;
            for (MetanomeInput input : inputs) {
                try {
                    input.close();
                } catch (IOException e) {
                    if (failure == null) failure = e;
                    else failure.addSuppressed(e);
                }
            }
            try (var paths = Files.walk(directory)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        if (failure == null) failure = e;
                        else failure.addSuppressed(e);
                    }
                }
            } catch (IOException e) {
                if (failure == null) failure = e;
                else failure.addSuppressed(e);
            }
            if (failure != null) throw failure;
        }
    }

    @Override
    public String getAuthors() {
        return "Jakob L. Mueller";
    }

    @Override
    public String getDescription() {
        return "SPIND: unary and n-ary partial inclusion dependency discovery";
    }
}
