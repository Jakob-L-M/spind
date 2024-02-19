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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Spind {
    Config config;
    int[] relationOffsets;

    public Spind(Config config) {
        this.config = config;
    }

    public void execute() throws IOException {
        List<RelationalInput> inputs = openInputs();

        // 1) get all unary attributes.
        Attribute[] attributes = initAttributes(inputs);

        // 2) init the helper Classes
        Candidates candidates = new Candidates();
        Sorter sorter = new Sorter(100_000_000);

        // load the unary candidates
        candidates.loadUnary(attributes);

        HashMap<String, List<String>> output = new HashMap<>();

        // 3) while attributes not empty.
        while (attributes.length > 0) {
            candidates.layer++;

            if (candidates.layer > 1) inputs = openInputs(attributes);

            System.out.print("Layer: " + candidates.layer + " | Attributes: " + attributes.length);
            // 3.1) Load all attributes of the candidates.
            for (RelationalInput input : inputs) {
                sorter.process(input, config, attributes);
                input.close();
            }


            // 3.2) Validate candidates.
            Validator validator = new Validator(config, attributes, candidates);
            validator.validate();

            // remove all dependant candidates, that do not reference any attribute
            candidates.cleanCandidates();

            int unary = candidates.current.keySet().stream().mapToInt(x -> candidates.current.get(x).size()).sum();
            System.out.println(" | pINDs: " + unary);

            safePINDs(candidates, output, inputs, attributes);
            // 3.3) Clean up files.

            // 3.4) Generate new attributes for next layer.
            attributes = candidates.generateNextLayer(attributes, relationOffsets);
        }


        // 4) Save the output
    }

    private void safePINDs(Candidates candidates, HashMap<String, List<String>> output, List<RelationalInput> inputs, Attribute[] attributes) {
        for (int depId : candidates.current.keySet()) {
            Attribute dep = attributes[depId];
            String[] depHeader = inputs.get(dep.getRelationId()).headerLine;
            String depRelationName = inputs.get(dep.getRelationId()).relationName;
            StringBuilder depString = new StringBuilder();
            depString.append("(");
            for (int col : dep.getContainedColumns()) {
                depString.append(depRelationName)
                        .append(".")
                        .append(depHeader[col])
                        .append(",");
            };
            depString.append(")");

            for (int refId : candidates.current.get(depId).keySet()) {
                Attribute ref = attributes[refId];
                String[] refheader = inputs.get(ref.getRelationId()).headerLine;
                String refRelationName = inputs.get(ref.getRelationId()).relationName;
                StringBuilder refString = new StringBuilder();
                refString.append("(");
                for (int col : ref.getContainedColumns()) {
                    refString.append(refRelationName)
                            .append(".")
                            .append(refheader[col])
                            .append(",");
                };
                refString.append(")");
                output.computeIfAbsent(refString.toString(), k -> new ArrayList<>());
                output.get(refString.toString()).add(depString.toString());
            }
        }
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
            if (Arrays.stream(attributes).noneMatch(x -> x.getRelationId() == finalRelationId)) continue;

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
