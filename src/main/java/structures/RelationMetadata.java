package structures;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import io.RelationalInput;
import io.RowInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Base64;
import java.nio.charset.StandardCharsets;
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
    public final RowInput relationalInput;
    private final Config config;
    private final Logger logger = LoggerFactory.getLogger(RelationMetadata.class);
    private final long size;
    public String[] columnNames;
    public List<Attribute> connectedAttributes;

    public RelationMetadata(int relationId, int relationOffset, Path relationPath, Config config) throws IOException, CsvValidationException {
        this(relationId, relationOffset, new RelationalInput(relationPath, config), Files.size(relationPath), config);
    }

    public RelationMetadata(int relationId, int relationOffset, RowInput input, long size, Config config) {
        this.chunks = new ArrayList<>();
        this.config = config;
        this.id = relationId;
        this.size = size;
        this.offset = relationOffset;
        this.relationalInput = input;
        this.columnNames = input.columnNames();
    }

    /**
     * Splits the input horizontally into chunks. Splitting and merging the chunks enables more multi-threading
     * opportunities.
     */
    @Override
    public Void call() throws Exception {
        long sTime = System.currentTimeMillis();
        int maxSize = Math.max(10, config.CHUNK_SIZE / columnNames.length);
        int chunkNum = 0;
        Path chunkPath = Path.of(config.tempFolder + File.separator + "r_" + id + "_c_" + chunkNum + ".txt");
        try (RowInput input = relationalInput) {
            do {
                chunks.add(chunkPath);
                try (ICSVWriter csvWriter = new CSVWriterBuilder(Files.newBufferedWriter(chunkPath))
                        .withSeparator(config.separator).withQuoteChar(config.quoteChar).withEscapeChar(config.fileEscape).build()) {
                    int chunkSize = 0;
                    while (chunkSize++ < maxSize && input.hasNext()) {
                        String[] row = input.next();
                        if (row.length != columnNames.length) {
                            throw new IOException("Incorrect row width in relation " + id);
                        }
                        if (config.encodeChunkValues) {
                            for (int i = 0; i < row.length; i++) {
                                row[i] = row[i] == null ? "N" : "V" + Base64.getEncoder()
                                        .encodeToString(row[i].getBytes(StandardCharsets.UTF_8));
                            }
                        }
                        csvWriter.writeNext(row);
                        if (csvWriter.checkError()) {
                            throw new IOException("Could not write chunk " + chunkPath, csvWriter.getException());
                        }
                    }
                }
                chunkPath = Path.of(config.tempFolder + File.separator + "r_" + id + "_c_" + (++chunkNum) + ".txt");
            } while (input.hasNext());
        }
        logger.debug("Finished relation" + this.id + " (" + (System.currentTimeMillis() - sTime) + "ms)");
        return null;
    }

    @Override
    public int compareTo(RelationMetadata o) {
        // reverse comparison for descending sort
        return Long.compare(o.size, this.size);
    }
}
