package core;

import io.RelationalInput;
import runner.Config;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Spind {
    Config config;

    public void execute() throws IOException {
        List<RelationalInput> inputs = openInputs();
        // 1) get all unary attributes.
        // 2) create unary candidates.

        // 3) while candidates not empty.

        // 3.1) Load all attributes of the candidates.
        // 3.2) Validate candidates.
        // 3.3) Clean up files.
        // 3.4) Generate new candidates for next layer.

        // 4) Save the output
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
            relationOffset += inputs.getLast().attributes.size();
        }
        return inputs;
    }
}
