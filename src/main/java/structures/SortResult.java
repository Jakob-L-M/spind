package structures;

import java.util.List;

public record SortResult(MergeJob mergeJob, List<Attribute> connectedAttributes) {}
