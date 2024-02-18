package structures;

import java.util.*;

/**
 * Manages candidate creation and pruning.
 */
public class Candidates {
    public Map<Integer, HashMap<Integer, Long>> current; //TODO use a single link list instead of an inner HashMap
    HashMap<Integer, HashMap<Integer, List<Integer>>> unary;
    int layer = 1;

    /**
     * Prunes the current candidates
     *
     * @param valueGroup the group of attributes which all share some value.
     */
    public void prune(HashMap<Integer, Long> valueGroup) {
        for (int dependantAttributeId : valueGroup.keySet()) {
            long occurrences = valueGroup.get(dependantAttributeId);
            HashMap<Integer, Long> referenced = current.get(dependantAttributeId);
            for (int referencedAttributeId : referenced.keySet().stream().toList()) {
                if (valueGroup.containsKey(referencedAttributeId)) continue;

                if (referenced.compute(referencedAttributeId, (k, v) -> v - occurrences) < 0) {
                    referenced.remove(referencedAttributeId);
                }
            }
        }
    }

    /**
     * Using the current candidates, it produces a set of new candidates for the next layer.
     *
     * @return A list of all Attributes, which are present in at least one candidate pair.
     */
    public Attribute[] generateNextLayer(Attribute[] attributes) {
        if (layer == 1) {
            // safe unary attributes
            unary = new HashMap<>();
            for (int dependentAttribute : current.keySet()) {
                unary.computeIfAbsent(attributes[dependentAttribute].getRelationId(), k -> new HashMap<>());
                unary.get(attributes[dependentAttribute].getRelationId())
                        .put(dependentAttribute, current.get(dependentAttribute).keySet().stream().toList());
            }
        }
        // generate next layer by the method proposed in the BINDER paper
        Map<Attribute, Integer> nextAttributes = new HashMap<>();
        Map<Integer, HashMap<Integer, Long>> nextCandidates = new HashMap<>();
        int nextAttributeId = 0;
        for (int dependantNaryId : current.keySet()) {
            Attribute dependantNaryAttribute = attributes[dependantNaryId];
            // condition 1: the expansion needs to be from the same relation
            for (int unaryDepExpansionId : unary.get(dependantNaryAttribute.getRelationId()).keySet()) {
                // condition 2/3: the position of the expansion must be greater than all already contained ids
                if (unaryDepExpansionId <= Arrays.stream(dependantNaryAttribute.getContainedColumns()).max().orElse(-1)) {
                    continue;
                }

                for (int referentId : current.get(dependantNaryId).keySet()) {
                    Attribute referentNaryAttribute = attributes[referentId];
                    for (int unaryRefExpansionId : unary.get(dependantNaryAttribute.getRelationId()).get(unaryDepExpansionId)) {
                        // condition 1: the expansion needs to be from the same relation
                        if (attributes[unaryRefExpansionId].getRelationId() != referentNaryAttribute.getRelationId()) {
                            continue;
                        }
                        // condition 3: the expansion cannot be in the set that should be expanded.
                        if (Arrays.stream(referentNaryAttribute.getContainedColumns()).anyMatch(x -> x == unaryRefExpansionId)) {
                            continue;
                        }

                        // the two expansions cannot overlap
                        if (Arrays.stream(dependantNaryAttribute.getContainedColumns()).anyMatch(x -> x == unaryRefExpansionId)) {
                            continue;
                        }

                        // valid candidate found
                        Attribute dependant = new Attribute(-1,
                                dependantNaryAttribute.getRelationId(),
                                Arrays.copyOf(dependantNaryAttribute.getContainedColumns(), dependantNaryAttribute.getContainedColumns().length + 1));
                        dependant.getContainedColumns()[dependant.getContainedColumns().length - 1] = unaryDepExpansionId;

                        if (!nextAttributes.containsKey(dependant)) {
                            dependant.setId(nextAttributeId);
                            nextAttributes.put(dependant, nextAttributeId);
                            nextAttributeId++;
                        }
                        dependant.setId(nextAttributes.get(dependant));

                        Attribute referred = new Attribute(-1,
                                referentNaryAttribute.getRelationId(),
                                Arrays.copyOf(referentNaryAttribute.getContainedColumns(), referentNaryAttribute.getContainedColumns().length + 1));
                        referred.getContainedColumns()[referred.getContainedColumns().length - 1] = unaryRefExpansionId;

                        if (!nextAttributes.containsKey(referred)) {
                            referred.setId(nextAttributeId);
                            nextAttributes.put(referred, nextAttributeId);
                            nextAttributeId++;
                        }
                        referred.setId(nextAttributes.get(referred));

                        nextCandidates.computeIfAbsent(referred.getId(), k -> new HashMap<>());
                        nextCandidates.get(referred.getId()).put(dependant.getId(), -1L); // violations are handled elsewhere
                    }

                }
            }
        }
        Attribute[] nextAttributeIndex = new Attribute[nextAttributeId];
        for (Attribute attribute : nextAttributes.keySet()) {
            nextAttributeIndex[attribute.getId()] = attribute;
        }
        return nextAttributeIndex;
    }

    /**
     * Given the attribute index, this method generates the current candidates.
     *
     * @param attributes the current attribute index
     */
    public void loadUnary(Attribute[] attributes) {
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
