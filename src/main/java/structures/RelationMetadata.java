package structures;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import io.RelationalInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Relational Metadata includes the relations name, the column names and the paths to the relation chunks. All of these
 * attributes a loaded/constructed once, before the actual algorithm starts. The Metadata is kept in main memory the
 * whole time and is required to load each layer and for the creation of a human-readable output.
 */
public class RelationMetadata implements Callable<Void>, Comparable<RelationMetadata> {

    public final List<Path> chunks;
    public final int id;
    public final int offset;
    private final RelationalInput relationalInput;
    private final Config config;
    private final Logger logger = LoggerFactory.getLogger(RelationMetadata.class);
    private final long size;
    public String[] columnNames;
    public List<Attribute> connectedAttributes;

    public RelationMetadata(int relationId, int relationOffset, Path relationPath, Config config) throws IOException, CsvValidationException {
        this.chunks = new ArrayList<>();
        this.config = config;
        this.id = relationId;
        this.size = Files.size(relationPath);
        this.offset = relationOffset;
        this.relationalInput = new RelationalInput(relationPath, config);
        this.columnNames = relationalInput.headerLine;
    }

    /**
     * Splits the input horizontally into chunks. Splitting and merging the chunks enables more multi-threading
     * opportunities.
     */
    @Override
    public Void call() throws Exception {
        long sTime = System.currentTimeMillis();
        int maxSize = Math.max(10, config.CHUNK_SIZE / relationalInput.headerLine.length);
        int chunkNum = 0;
        Path chunkPath = Path.of(config.tempFolder + File.separator + "r_" + id + "_c_" + chunkNum + ".txt");
        BufferedWriter chunkWriter = Files.newBufferedWriter(chunkPath, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
        ICSVWriter csvWriter = new CSVWriterBuilder(chunkWriter).withSeparator(config.separator).withQuoteChar(config.quoteChar).withEscapeChar(config.fileEscape).build();
        chunks.add(chunkPath);
        int chunkSize = 0;

        while (relationalInput.hasNext()) {
            csvWriter.writeNext(relationalInput.next());
            if (++chunkSize >= maxSize) {
                chunkWriter.close();

                // open next chunk writer if there are still lines left
                if (relationalInput.hasNext()) {
                    chunkNum++;
                    chunkSize = 0;
                    chunkPath = Path.of(config.tempFolder + File.separator + "r_" + id + "_c_" + chunkNum + ".txt");
                    chunkWriter = Files.newBufferedWriter(chunkPath, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
                    csvWriter = new CSVWriterBuilder(chunkWriter).withSeparator(config.separator).withQuoteChar(config.quoteChar).withEscapeChar(config.fileEscape).build();
                    chunks.add(chunkPath);
                }
            }
        }
        chunkWriter.close();
        logger.debug("Finished relation" + this.id + " (" + (System.currentTimeMillis() - sTime) + "ms)");
        return null;
    }

    @Override
    public int compareTo(RelationMetadata o) {
        // reverse comparison for descending sort
        return this.size > o.size ? -1 : 1;
    }
}
