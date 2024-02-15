package structures;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Manages candidate creation and pruning.
 */
public class Candidates {
    HashMap<Integer, List<Integer>> unary;
    HashMap<Integer, Set<Integer>> current;

    /**
     * Prunes the current candidates
     */
    public void prune() {

    }

    /**
     * Using the current candidates, it produces a set of new candidates for the next layer.
     * @return A list of all Attributes, which are present in at least one candidate pair.
     */
    public List<Attribute> generateNextLayer() {
        return null;
    }
}
