package structures;

import java.nio.file.Path;
import java.util.List;

public record SortJob(Path chunkPath, List<Attribute> connectedAttributes) {}
