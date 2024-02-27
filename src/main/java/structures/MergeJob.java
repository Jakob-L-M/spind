package structures;

import java.nio.file.Path;
import java.util.List;

public record MergeJob(List<Path> chunkPaths, int relationId, Path to) {}
