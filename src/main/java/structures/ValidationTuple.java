package structures;

import java.util.HashMap;

public record ValidationTuple(HashMap<Integer, Long> attributeGroup, Long hash) {}
