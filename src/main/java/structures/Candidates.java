package structures;

import java.util.*;

/**
 * Manages candidate creation and pruning.
 */
public class Candidates {
    public Map<Integer, HashMap<Integer, Long>> current; //TODO use a single link list instead of an inner HashMap
    public int layer = 0;
    private int nextAttributeId;
    HashMap<Integer, HashMap<Integer, HashMap<Integer, List<Integer>>>> unary;

    /**
     * Prunes the current candidates
     *
     * @param valueGroup the group of attributes which all share some value.
     */
    public void prune(HashMap<Integer, Long> valueGroup) {
        for (int dependantAttributeId : valueGroup.keySet()) {
            if (!current.containsKey(dependantAttributeId)) {
                // the attribute does not depend on any other attribute.
                continue;
            }
            long occurrences = valueGroup.get(dependantAttributeId);
            HashMap<Integer, Long> referenced = current.get(dependantAttributeId);
            for (int referencedAttributeId : referenced.keySet().stream().toList()) {
                // if the valueGroup includes the referenced Attribute: no violation
                if (valueGroup.containsKey(referencedAttributeId)) continue;

                // not null since we iterate over the key set
                if (referenced.compute(referencedAttributeId, (k, v) -> v - occurrences) < 0) {
                    referenced.remove(referencedAttributeId);
                }
            }
            if (referenced.isEmpty()) current.remove(dependantAttributeId);
        }
    }

    /**
     * Using the current candidates, it produces a set of new candidates for the next layer.
     *
     * @return A list of all Attributes, which are present in at least one candidate pair.
     */
    public Attribute[] generateNextLayer(Attribute[] attributes, int[] relationOffsets) {
        if (layer == 1) {
            // safe unary attributes
            storeUnary(attributes, relationOffsets);
        }
        // generate next layer by the method proposed in the BINDER paper
        HashMap<Attribute, Integer> nextAttributes = new HashMap<>();
        HashMap<Integer, HashMap<Integer, Long>> nextCandidates = new HashMap<>();
        this.nextAttributeId = 0;
        for (int naryDepId : current.keySet()) {
            Attribute naryDepAttribute = attributes[naryDepId];
            int depRelationId = naryDepAttribute.getRelationId();
            // condition 1: the expansion needs to be from the same relation
            for (int unaryDepExpansionId : unary.get(depRelationId).keySet()) {
                // condition 2/3: the position of the expansion must be greater than all already contained ids
                if (unaryDepExpansionId <= Arrays.stream(naryDepAttribute.getContainedColumns()).max().orElse(-1)) {
                    continue;
                }

                for (int naryRefId : current.get(naryDepId).keySet()) {
                    Attribute naryRefAttribute = attributes[naryRefId];
                    int refRelationId = naryRefAttribute.getRelationId();

                    // condition 1: the referenced expansion needs to be from the same relation as the nary referenced attribute
                    if (!unary.get(depRelationId).get(unaryDepExpansionId).containsKey(refRelationId)) {
                        continue;
                    }

                    for (int unaryRefExpansionId : unary.get(depRelationId).get(unaryDepExpansionId).get(refRelationId)) {

                        // condition 3: the expansion cannot be in the set that should be expanded.
                        if (Arrays.stream(naryRefAttribute.getContainedColumns()).anyMatch(x -> x == unaryRefExpansionId)) {
                            continue;
                        }

                        // the two expansions cannot overlap
                        if (refRelationId == depRelationId)
                        {
                            if (Arrays.stream(naryDepAttribute.getContainedColumns()).anyMatch(x -> x == unaryRefExpansionId)) {
                                continue;
                            }
                            if (Arrays.stream(naryRefAttribute.getContainedColumns()).anyMatch(x -> x == unaryDepExpansionId)) {
                                continue;
                            }
                            if (unaryRefExpansionId == unaryDepExpansionId) {
                                continue;
                            }
                        }


                        // valid candidate found
                        Attribute dependant = generateAttribute(naryDepAttribute, unaryDepExpansionId, nextAttributes);

                        Attribute referenced = generateAttribute(naryRefAttribute, unaryRefExpansionId, nextAttributes);

                        nextCandidates.computeIfAbsent(dependant.getId(), k -> new HashMap<>());
                        nextCandidates.get(dependant.getId()).put(referenced.getId(), 0L); // violations are handled elsewhere
                    }

                }
            }
        }
        Attribute[] nextAttributeIndex = new Attribute[nextAttributeId];
        for (Attribute attribute : nextAttributes.keySet()) {
            nextAttributeIndex[attribute.getId()] = attribute;
        }
        current = nextCandidates;
        return nextAttributeIndex;
    }

    public void cleanCandidates() {
        current.keySet().removeIf(integer -> current.get(integer).isEmpty());
    }

    /**
     * Stores the unary pINDs so that they can be used to expand the n-ary attributes in the higher layers.
     * Unary pINDs are stored as follows:
     * [RelationId] maps to -> [DependantColumn] maps to -> [RelationId of reference] contains list of column numbers.
     *
     * @param attributes      The attribute index of all attributes with size 1.
     * @param relationOffsets The id to column number offsets for each relation.
     */
    private void storeUnary(Attribute[] attributes, int[] relationOffsets) {
        unary = new HashMap<>();
        for (int dependentAttribute : current.keySet()) {
            int dependentRelationId = attributes[dependentAttribute].getRelationId();
            unary.computeIfAbsent(dependentRelationId, k -> new HashMap<>());
            HashMap<Integer, HashMap<Integer, List<Integer>>> relationMap = unary.get(dependentRelationId);
            HashMap<Integer, List<Integer>> referredAttributes = new HashMap<>();
            for (int referencedAttribute : current.get(dependentAttribute).keySet()) {
                int referencedRelationId = attributes[referencedAttribute].getRelationId();
                referredAttributes.computeIfAbsent(referencedRelationId, k -> new ArrayList<>());

                referredAttributes.get(referencedRelationId).add(referencedAttribute - relationOffsets[referencedRelationId]);
            }
            relationMap.put(dependentAttribute - relationOffsets[dependentRelationId], referredAttributes);
        }
    }

    private Attribute generateAttribute(Attribute naryAttribute, int unaryExpansionId, HashMap<Attribute, Integer> nextAttributes) {
        Attribute attribute = new Attribute(-1,
                naryAttribute.getRelationId(),
                Arrays.copyOf(naryAttribute.getContainedColumns(), naryAttribute.getContainedColumns().length + 1));
        attribute.getContainedColumns()[naryAttribute.getContainedColumns().length] = unaryExpansionId;

        if (!nextAttributes.containsKey(attribute)) {
            attribute.setId(nextAttributeId);
            nextAttributes.put(attribute, nextAttributeId);
            nextAttributeId++;
        } else {
            attribute.setId(nextAttributes.get(attribute));
        }

        return attribute;
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

                dependantMap.put(referredAttribute.id, 0L);//dependantAttribute.metadata.possibleViolations);
            }
            current.put(dependantAttribute.id, dependantMap);
        }
    }
}
