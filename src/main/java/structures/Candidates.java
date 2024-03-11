package structures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;

import java.util.*;

/**
 * Manages candidate creation and pruning.
 */
public class Candidates {
    private final Config config;
    private final Logger logger = LoggerFactory.getLogger(Candidates.class);
    public Map<Integer, PINDList> current;
    HashMap<Integer, HashMap<Integer, HashMap<Integer, List<Integer>>>> unary;
    private int nextAttributeId;

    public Candidates(Config config) {
        this.config = config;
    }

    /**
     * Prunes the current candidates
     *
     * @param valueGroup the group of attributes which all share some value.
     */
    public void prune(Map<Integer, Long> valueGroup) {
        for (int dependantAttributeId : valueGroup.keySet()) {
            if (!current.containsKey(dependantAttributeId)) {
                // the attribute does not depend on any other attribute.
                continue;
            }
            long occurrences;
            if (config.duplicateHandling == Config.DuplicateHandling.AWARE) {
                occurrences = valueGroup.get(dependantAttributeId);
            } else {
                occurrences = 1L;
            }
            PINDList.PINDIterator referenced = current.get(dependantAttributeId).elementIterator();
            while (referenced.hasNext()){
                PINDList.PINDElement referencedAttribute = referenced.next();
                // if the valueGroup includes the referenced Attribute: no violation
                if (valueGroup.containsKey(referencedAttribute.referenced)) continue;

                // not null since we iterate over the key set
                if (referencedAttribute.violate(occurrences) < 0) {
                    referenced.remove();
                }
            }
            if (current.get(dependantAttributeId).isEmpty()) current.remove(dependantAttributeId);
        }
    }

    /**
     * Using the current candidates, it produces a set of new candidates for the next layer.
     *
     * @return A list of all Attributes, which are present in at least one candidate pair.
     */
    public Attribute[] generateNextLayer(Attribute[] attributes, RelationMetadata[] relationMetadata, int layer) {
        HashMap<String, Set<String>> lookUp;
        if (layer == 1) {
            // safe unary attributes
            storeUnary(attributes, relationMetadata);
            lookUp = new HashMap<>();
        } else {
            lookUp = createLookUps(attributes);
        }
        // generate next layer by the method proposed in the BINDER paper
        HashMap<Attribute, Integer> nextAttributes = new HashMap<>();
        HashMap<Integer, PINDList> nextCandidates = new HashMap<>();
        this.nextAttributeId = 0;
        for (int naryDepId : current.keySet()) {
            Attribute naryDepAttribute = attributes[naryDepId];

            if (naryDepAttribute.getMetadata().totalValues == 0) {
                continue;
            }

            int depRelationId = naryDepAttribute.getRelationId();
            // condition 1: the expansion needs to be from the same relation
            for (int unaryDepColumn : unary.get(depRelationId).keySet()) {
                // condition 2/3: the position of the expansion must be greater than all already contained ids
                if (unaryDepColumn <= Arrays.stream(naryDepAttribute.getContainedColumns()).max().orElse(-1)) {
                    continue;
                }

                PINDList.PINDIterator naryRef = current.get(naryDepId).elementIterator();
                while (naryRef.hasNext()) {
                    int naryRefId = naryRef.next().referenced;
                    Attribute naryRefAttribute = attributes[naryRefId];
                    int refRelationId = naryRefAttribute.getRelationId();

                    // condition 1: the referenced expansion needs to be from the same relation as the nary referenced attribute
                    if (!unary.get(depRelationId).get(unaryDepColumn).containsKey(refRelationId)) {
                        continue;
                    }

                    for (int unaryRefColumn : unary.get(depRelationId).get(unaryDepColumn).get(refRelationId)) {

                        // condition 3: the expansion cannot be in the set that should be expanded.
                        if (Arrays.stream(naryRefAttribute.getContainedColumns()).anyMatch(x -> x == unaryRefColumn)) {
                            continue;
                        }

                        // the two expansions cannot overlap
                        if (refRelationId == depRelationId) {
                            if (Arrays.stream(naryDepAttribute.getContainedColumns()).anyMatch(x -> x == unaryRefColumn)) {
                                continue;
                            }
                            if (Arrays.stream(naryRefAttribute.getContainedColumns()).anyMatch(x -> x == unaryDepColumn)) {
                                continue;
                            }
                            if (unaryRefColumn == unaryDepColumn) {
                                continue;
                            }
                        }

                        int[] dependantColumns = Arrays.copyOf(naryDepAttribute.containedColumns, layer + 1);
                        dependantColumns[layer] = unaryDepColumn;

                        int[] referencedColumns = Arrays.copyOf(naryRefAttribute.containedColumns, layer + 1);
                        referencedColumns[layer] = unaryRefColumn;

                        if (layer > 1 && !possibleCandidate(dependantColumns, referencedColumns, depRelationId, refRelationId, lookUp, layer)) {
                            continue;
                        }

                        // valid candidate found
                        Attribute dependant = generateAttribute(naryDepAttribute, unaryDepColumn, nextAttributes);

                        Attribute referenced = generateAttribute(naryRefAttribute, unaryRefColumn, nextAttributes);

                        PINDList referencedList = nextCandidates.computeIfAbsent(dependant.getId(), k -> new PINDList());
                        referencedList.add(referenced.getId(), 0); // violations are handled elsewhere
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

    private HashMap<String, Set<String>> createLookUps(Attribute[] attributes) {
        HashMap<String, Set<String>> lookup = new HashMap<>(current.size());
        for (int depId : current.keySet()) {
            String depString = attributes[depId].toString();
            HashSet<String> depSet = new HashSet<>();
            PINDList.PINDIterator referencedList = current.get(depId).elementIterator();
            while (referencedList.hasNext()){
                int refId = referencedList.next().referenced;
                String refString = attributes[refId].toString();
                depSet.add(refString);
            }
            lookup.put(depString, depSet);
        }
        return lookup;
    }

    private boolean possibleCandidate(int[] dependantColumns, int[] referencedColumns, int depRelationId, int refRelationId, HashMap<String, Set<String>> lookup, int layer) {
        // a candidate can only be valid, if all included partitions of size-1 have been validated.
        // we do not need to skip the last position, since that one is always possible
        for (int skipIndex = 0; skipIndex < layer; skipIndex++) {
            StringBuilder depString = new StringBuilder();
            StringBuilder refString = new StringBuilder();

            depString.append(depRelationId).append(": [");
            refString.append(refRelationId).append(": [");

            int attPointer = 0;
            for (int col = 0; col < layer; col++) {
                if (col == skipIndex) {
                    attPointer++;
                }
                depString.append(dependantColumns[attPointer]).append(", ");
                refString.append(referencedColumns[attPointer]).append(", ");

                attPointer++;
            }
            depString.delete(depString.length() - 2, depString.length()).append("]");
            refString.delete(refString.length() - 2, refString.length()).append("]");

            Set<String> refSet = lookup.get(depString.toString());
            if (refSet == null || !refSet.contains(refString.toString())) {
                logger.debug("Skipped candidate. " + depRelationId + ": " + Arrays.toString(dependantColumns) + " -> "
                        + refRelationId + ": " + Arrays.toString(referencedColumns));
                return false;
            }
        }
        return true;
    }

    public void cleanCandidates() {
        current.keySet().removeIf(integer -> current.get(integer).isEmpty());
    }

    /**
     * Stores the unary pINDs so that they can be used to expand the n-ary attributes in the higher layers.
     * Unary pINDs are stored as follows:
     * [RelationId] maps to -> [DependantColumn] maps to -> [RelationId of reference] contains list of column numbers.
     *
     * @param attributes       The attribute index of all attributes with size 1.
     * @param relationMetadata Metadata that is used to access the relation offsets
     */
    private void storeUnary(Attribute[] attributes, RelationMetadata[] relationMetadata) {
        unary = new HashMap<>();
        for (int dependentAttribute : current.keySet()) {

            if (attributes[dependentAttribute].getMetadata().totalValues == 0) {
                // do not safe attributes which are completely empty. They do not carry meaning.
                continue;
            }

            int dependentRelationId = attributes[dependentAttribute].getRelationId();

            HashMap<Integer, HashMap<Integer, List<Integer>>> relationMap = unary.computeIfAbsent(dependentRelationId, k -> new HashMap<>());

            HashMap<Integer, List<Integer>> referredAttributes = new HashMap<>();

            PINDList.PINDIterator referencedIterator = current.get(dependentAttribute).elementIterator();
            while (referencedIterator.hasNext()){

                int referencedAttribute = referencedIterator.next().referenced;

                if (attributes[referencedAttribute].getMetadata().totalValues == 0) {
                    // do not safe attributes which are completely empty. They do not carry meaning.
                    continue;
                }

                int referencedRelationId = attributes[referencedAttribute].getRelationId();
                referredAttributes.computeIfAbsent(referencedRelationId, k -> new ArrayList<>());

                referredAttributes.get(referencedRelationId).add(referencedAttribute - relationMetadata[referencedRelationId].relationOffset);
            }
            relationMap.put(dependentAttribute - relationMetadata[dependentRelationId].relationOffset, referredAttributes);
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
        List<Integer> attributeIds = Arrays.stream(attributes).map(Attribute::getId).toList();
        for (Attribute dependantAttribute : attributes) {
            current.put(dependantAttribute.id, new PINDList(0L, attributeIds, dependantAttribute.getId()));
        }
    }

    public void pruneGlobalUnique(Attribute[] attributes) {
        int numPruned = 0;
        for (int dependantId : current.keySet()) {
            long depGlobalUnique = attributes[dependantId].getMetadata().globalUnique;

            if (depGlobalUnique == 0L) {
                continue;
            }

            PINDList refMap = current.get(dependantId);
            PINDList.PINDIterator referred = refMap.elementIterator();
            while (referred.hasNext()) {
                if (referred.next().violate(depGlobalUnique) < 0) {
                    referred.remove();
                    numPruned++;
                }
            }
        }
        logger.info("Pruned " + numPruned + " candidates through global uniqueness.");
    }

    public void pruneNull(Attribute[] attributes) {
        int numPruned = 0;
        // in subset and equality mode, we do not need to do anything
        if (config.nullHandling != Config.NullHandling.SUBSET && config.nullHandling != Config.NullHandling.EQUALITY) {

            for (int dependantId : current.keySet()) {
                long depNull = attributes[dependantId].getMetadata().nullEntries;

                PINDList.PINDIterator referenced = current.get(dependantId).elementIterator();
                while (referenced.hasNext()) {
                    PINDList.PINDElement ref = referenced.next();
                    long refNull = attributes[ref.referenced].getMetadata().nullEntries;

                    if (config.nullHandling == Config.NullHandling.FOREIGN) {
                        if (refNull > 0) {
                            // foreign mode does not allow the referenced side to have any nulls
                            referenced.remove();
                            numPruned++;
                        }
                    } else if (config.nullHandling == Config.NullHandling.INEQUALITY){
                        // Inequality mode: every null is different. Therefor all depNulls are violations
                        if(ref.violate(depNull) < 0) {
                            referenced.remove();
                            numPruned++;
                        }
                    }
                }
            }
        }
        logger.info("Pruned " + numPruned + " candidates through null constraints.");
    }

    public void calculateViolations(Attribute[] attributes) {
        for (int dependantId : current.keySet()) {
            long depSize;

            if (config.duplicateHandling == Config.DuplicateHandling.UNAWARE) {
                depSize = attributes[dependantId].getMetadata().uniqueValues;
            } else {
                depSize = attributes[dependantId].getMetadata().totalValues;
            }

            long maxViolations = (long) ((1.0 - config.threshold) * depSize);

            PINDList.PINDIterator referenced = current.get(dependantId).elementIterator();
            while (referenced.hasNext()) {
                referenced.next().violationsLeft = maxViolations;
            }
        }
    }
}
