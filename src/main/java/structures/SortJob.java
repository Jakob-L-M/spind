package structures;

import com.google.common.hash.BloomFilter;
import io.Sorter;
import org.slf4j.LoggerFactory;
import runner.Config;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

public final class SortJob implements Callable<SortResult>, Comparable<SortJob> {
    private final Path chunkPath;
    private final List<Attribute> connectedAttributes;
    private final int relationId;
    private final int sort;
    private final int chunk;
    private final Config config;
    private final BloomFilter<Integer> filter;
    private final int layer;

    public SortJob(Path chunkPath, List<Attribute> connectedAttributes, int relationId, int sortSize, int chunkSize, Config config, BloomFilter<Integer> filter, int layer)  {
        this.chunkPath = chunkPath;
        this.connectedAttributes = connectedAttributes;
        this.relationId = relationId;
        this.sort = sortSize;
        this.chunk = chunkSize;
        this.config = config;
        this.filter = filter;
        this.layer = layer;
    }

    public Path chunkPath() {
        return chunkPath;
    }

    public List<Attribute> connectedAttributes() {
        return connectedAttributes;
    }

    public int relationId() {
        return relationId;
    }



    @Override
    public String toString() {
        return "SortJob[" +
                "chunkPath=" + chunkPath + ", " +
                "connectedAttributes=" + connectedAttributes + ", " +
                "relationId=" + relationId + ']';
    }

    @Override
    public SortResult call() throws Exception {
        LoggerFactory.getLogger(SortJob.class).debug("Starting to sort: " + chunkPath + " with " + connectedAttributes.size() + " attributes");
        Sorter sorter = new Sorter(sort, (long) (connectedAttributes.size()) * 10 * chunk / sort);
        return sorter.process(this, config, filter, layer);
    }

    @Override
    public int compareTo(SortJob other) {
        return other.connectedAttributes.size() - this.connectedAttributes.size();
    }
}
