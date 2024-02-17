package structures;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages candidate creation and pruning.
 */
public class Candidates {
    Map<Integer, List<Integer>> unary;
    Map<Integer, HashMap<Integer, Long>> current;
    int layer = 1;

    /**
     * Prunes the current candidates
     * @param valueGroup the group of attributes which all share some value.
     */
    public void prune(HashMap<Integer, Long> valueGroup) {
        for (int dependantAttributeId : valueGroup.keySet()) {
            long occurrences =valueGroup.get(dependantAttributeId);
            HashMap<Integer, Long> referenced = current.get(dependantAttributeId);
            for (int referencedAttributeId : referenced.keySet()) {
                if (valueGroup.containsKey(referencedAttributeId)) continue;

                if (referenced.compute(referencedAttributeId, (k, v) -> v - occurrences) < 0) {
                    referenced.remove(referencedAttributeId);
                };
            }
        }
    }

    /**
     * Using the current candidates, it produces a set of new candidates for the next layer.
     *
     * @return A list of all Attributes, which are present in at least one candidate pair.
     */
    public List<Attribute> generateNextLayer() {
        return null;
    }

    /**
     * Given the attribute index, this method generates the current candidates.
     *
     * @param attributes the current attribute index
     */
    public void intiCurrentLayer(Attribute[] attributes) {
        if (layer == 1) {
            current = new HashMap<>(attributes.length);
            for (Attribute dependantAttribute : attributes) {
                HashMap<Integer, Long> dependantMap = new HashMap<>();
                for (Attribute referredAttribute : attributes) {
                    if (dependantAttribute.id == referredAttribute.id) continue;

                    dependantMap.put(referredAttribute.id, dependantAttribute.metadata.possibleViolations);
                }
                current.put(dependantAttribute.id, dependantMap);
            }
        }
    }
}
