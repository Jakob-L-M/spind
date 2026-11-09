package metanome;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.result_receiver.RelaxedInclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.RelaxedInclusionDependency;
import io.Output;
import runner.Config;
import structures.Attribute;
import structures.PINDList;
import structures.RelationMetadata;

import java.io.IOException;
import java.util.Arrays;

final class MetanomeOutput extends Output {
    private final RelaxedInclusionDependencyResultReceiver receiver;

    MetanomeOutput(RelaxedInclusionDependencyResultReceiver receiver) {
        super(null);
        this.receiver = receiver;
    }

    @Override
    public void storePINDs(RelationMetadata[] inputs, Attribute[] attributes, int layer, Config config) throws IOException {
        for (Attribute dependant : attributes) {
            if (dependant.getReferenced() == null) continue;
            PINDList.PINDIterator iterator = dependant.getReferenced().elementIterator();
            while (iterator.hasNext()) {
                PINDList.PINDElement reference = iterator.next();
                RelaxedInclusionDependency result = new RelaxedInclusionDependency(
                        columns(dependant, inputs, config), columns(attributes[reference.id], inputs, config),
                        getPartialDegree(config, reference));
                try {
                    receiver.receiveResult(result);
                } catch (AlgorithmExecutionException e) {
                    throw new IOException("Metanome could not receive SPIND result", e);
                }
            }
        }
    }

    private ColumnPermutation columns(Attribute attribute, RelationMetadata[] inputs, Config config) {
        int relation = attribute.getRelationId();
        return new ColumnPermutation(Arrays.stream(attribute.getContainedColumns())
                .mapToObj(column -> new ColumnIdentifier(config.tableNames[relation], inputs[relation].columnNames[column]))
                .toArray(ColumnIdentifier[]::new));
    }
}
