package structures;

import com.opencsv.CSVWriter;
import io.RelationalInput;
import runner.Config;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Relational Metadata includes the relations name, the column names and the paths to the relation chunks. All of these
 * attributes a loaded/constructed once, before the actual algorithm starts. The Metadata is kept in main memory the
 * whole time and is required to load each layer and for the creation of a human-readable output.
 */
public class RelationMetadata {

    public final int relationId;
    public final List<Path> chunks;
    private final RelationalInput relationalInput;
    public String[] columnNames;
    public final String relationName;
    public final int relationOffset;
    public List<Attribute> connectedAttributes;

    public RelationMetadata(String relationName, int relationId, int relationOffset, Path relationPath, Config config) throws IOException {
        this.chunks = new ArrayList<>();
        this.relationName = relationName;
        this.relationId = relationId;
        this.relationOffset = relationOffset;
        this.relationalInput = new RelationalInput(relationPath, config);
        this.columnNames = relationalInput.headerLine;
    }

    /**
     * Splits the input horizontally into chunks. Splitting and merging the chunks enables more multi-threading
     * opportunities.
     *
     * @param maxSize The maximal number of lines each chunk should contain.
     */
    public void createChunks(int maxSize, Config config) throws IOException {
        int chunkNum = 0;
        Path chunkPath = Path.of(config.tempFolder + File.separator + "r_" + relationId + "_c_" + chunkNum + ".txt");
        BufferedWriter chunkWriter = Files.newBufferedWriter(chunkPath, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
        CSVWriter  csvWriter = new CSVWriter(chunkWriter);
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
                    chunkPath = Path.of(config.tempFolder + File.separator + "r_" + relationId + "_c_" + chunkNum + ".txt");
                    chunkWriter = Files.newBufferedWriter(chunkPath, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
                    csvWriter = new CSVWriter(chunkWriter);
                    chunks.add(chunkPath);
                }
            }
        }
        chunkWriter.close();
    }
}
