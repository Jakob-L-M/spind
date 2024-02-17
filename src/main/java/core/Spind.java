package core;

import io.RelationalInput;
import io.Sorter;
import io.Validator;
import runner.Config;
import structures.Attribute;
import structures.Candidates;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Spind {
    Config config;

    public Spind(Config config) {
        this.config = config;
    }

    public void execute() throws IOException {
        List<RelationalInput> inputs = openInputs();

        // 1) get all unary attributes.
        Attribute[] attributes = initAttributes(inputs);

        // 2) init the helper Classes
        Candidates candidates = new Candidates();
        Sorter sorter = new Sorter(100_000);

        // 3) while attributes not empty.

        // 3.1) Load all attributes of the candidates.
        for (RelationalInput input : inputs) {
            sorter.process(input, config, attributes);
        }

        // 3.1) Set up the current candidate maps.
        candidates.intiCurrentLayer(attributes);

        // 3.2) Validate candidates.
        Validator validator = new Validator(config, attributes, candidates);
        validator.validate();

        // 3.3) Clean up files.

        // 3.4) Generate new attributes for next layer.

        // 4) Save the output
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

        int relationOffset = 0;
        for (int relationId = 0; relationId < config.tableNames.length; relationId++) {
            inputs.add(new RelationalInput(config.tableNames[relationId],
                    config.folderPath + File.separator + config.databaseName + File.separator + config.tableNames[relationId] + config.fileEnding,
                    config,
                    relationOffset,
                    relationId
                    )
            );
            relationOffset += inputs.get(inputs.size() - 1).attributes.size();
        }
        return inputs;
    }
}
