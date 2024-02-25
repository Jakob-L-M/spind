package core;

import io.Output;
import io.RelationalInput;
import io.Sorter;
import io.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.Attribute;
import structures.Candidates;
import structures.Clock;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Spind {
    Config config;
    int[] relationOffsets;
    Logger logger;
    Output output;
    Clock clock;

    public Spind(Config config) {
        this.config = config;
        this.output = new Output(config.resultFolder);
        this.clock = new Clock();
        clock.start("total");
        this.logger = LoggerFactory.getLogger(Spind.class);
    }

    public void execute() throws IOException {
        List<RelationalInput> inputs = openInputs();

        // 1) get all unary attributes.
        Attribute[] attributes = initAttributes(inputs);

        // 2) init the helper Classes
        Candidates candidates = new Candidates();
        Sorter sorter = new Sorter(3_000_000);

        // load the unary candidates
        candidates.loadUnary(attributes);

        // 3) while attributes not empty.
        while (attributes.length > 0) {
            candidates.layer++;

            if (candidates.layer > 1) inputs = openInputs(attributes);

            logger.info(" Starting layer: " + candidates.layer + " with " + attributes.length + " attributes forming " + candidates.current.keySet().stream().mapToInt(x -> candidates.current.get(x).size()).sum());
            // 3.1) Load all attributes of the candidates.
            for (RelationalInput input : inputs) {
                if (input == null) continue;
                sorter.process(input, config, attributes);
                input.close();
            }


            // 3.2) Validate candidates.
            Validator validator = new Validator(config, attributes, candidates);
            validator.validate();

            // remove all dependant candidates, that do not reference any attribute
            candidates.cleanCandidates();

            int unary = candidates.current.keySet().stream().mapToInt(x -> candidates.current.get(x).size()).sum();
            logger.info("Found " + unary + " pINDs at level " + candidates.layer);

            output.storePINDs(candidates, inputs, attributes);
            // 3.3) Clean up files.

            // 3.4) Generate new attributes for next layer.
            attributes = candidates.generateNextLayer(attributes, relationOffsets);
        }


        // 4) Save the output
        output.storeMetadata(config, clock);
    }

    private Attribute[] initAttributes(List<RelationalInput> inputs) {
        Attribute[] attributes = new Attribute[inputs.stream().mapToInt(x -> x.attributes.size()).sum()];
        for (RelationalInput input : inputs) {
            for (Attribute attribute : input.attributes) {
                attributes[attribute.getId()] = attribute;
            }
        }
        return attributes;
    }

    private List<RelationalInput> openInputs() throws IOException {

        List<RelationalInput> inputs = new ArrayList<>();
        this.relationOffsets = new int[config.tableNames.length];

        int relationOffset = 0;
        for (int relationId = 0; relationId < config.tableNames.length; relationId++) {
            inputs.add(new RelationalInput(config.tableNames[relationId],
                            config.folderPath + File.separator + config.databaseName + File.separator + config.tableNames[relationId] + config.fileEnding,
                            config,
                            relationOffset,
                            relationId
                    )
            );
            relationOffsets[relationId] = relationOffset;
            relationOffset += inputs.get(inputs.size() - 1).attributes.size();
        }
        return inputs;
    }

    private List<RelationalInput> openInputs(Attribute[] attributes) throws IOException {

        List<RelationalInput> inputs = new ArrayList<>();

        int relationOffset = 0;
        for (int relationId = 0; relationId < config.tableNames.length; relationId++) {
            int finalRelationId = relationId;

            // only load relation if it is present in at least one attribute
            if (Arrays.stream(attributes).noneMatch(x -> x.getRelationId() == finalRelationId)) {
                inputs.add(null);
                continue;
            }

            inputs.add(new RelationalInput(config.tableNames[relationId],
                            config.folderPath + File.separator + config.databaseName + File.separator + config.tableNames[relationId] + config.fileEnding,
                            config,
                            relationOffset,
                            relationId,
                            Arrays.stream(attributes).filter(a -> a.getRelationId() == finalRelationId).toList()
                    )
            );

            relationOffset += inputs.get(inputs.size() - 1).attributes.size();
        }
        return inputs;
    }
}
