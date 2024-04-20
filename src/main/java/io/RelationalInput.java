package io;

import com.google.common.hash.BloomFilter;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import runner.Config;
import structures.Attribute;
import structures.Hashing;
import structures.SortJob;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class RelationalInput {

    private final Config config;
    public List<Attribute> attributes;
    public String[] headerLine;
    protected CSVReader CSVReader;
    protected String[] nextLine;
    protected int currentLineNumber = -1; // Initialized to -1 because of lookahead
    protected int numberOfSkippedLines = 0;
    private boolean chunkReader = true;

    public RelationalInput(Path relationPath, Config config) throws IOException {

        chunkReader = false;

        this.config = config;

        BufferedReader reader = Files.newBufferedReader(relationPath);

        this.CSVReader = new CSVReaderBuilder(reader)
                .withCSVParser(new CSVParserBuilder()
                        .withSeparator(config.separator)
                        .withEscapeChar(config.fileEscape)
                        .withIgnoreLeadingWhiteSpace(config.ignoreLeadingWhiteSpace)
                        .withStrictQuotes(config.strictQuotes)
                        .withQuoteChar(config.quoteChar)
                        .build())
                .build();

        // read the first line
        this.nextLine = readNextLine();

        if (config.inputFileHasHeader) {
            this.headerLine = this.nextLine;
            // move to the first proper line
            next();
        }

        // If the header is still null generate a standard header the size of number of columns.
        if (this.headerLine == null) {
            generateHeaderLine();
        }
    }

    /**
     * Constructor for reading a chuck file
     *
     * @param sortJob the chunk to be processed
     * @param config  the reading config
     */
    public RelationalInput(SortJob sortJob, Config config) throws IOException {
        this.config = config;

        BufferedReader reader = Files.newBufferedReader(sortJob.chunkPath());

        this.CSVReader = new CSVReaderBuilder(reader)
                .withCSVParser(new CSVParserBuilder().withQuoteChar(config.quoteChar).withSeparator(config.separator).build()).build();

        // read the first line
        this.nextLine = readNextLine();
        this.attributes = new ArrayList<>();
        for (Attribute connectedAttribute : sortJob.connectedAttributes()) {
            attributes.add(new Attribute(connectedAttribute.getId(), connectedAttribute.getRelationId(), connectedAttribute.getContainedColumns()));
        }
    }


    /**
     * Builds the string representation for every attribute combination of the given table and updates the attributes
     * accordingly.
     */
    public void updateAttributeCombinations(BloomFilter<Long> filter, int layer) {
        String[] values = next(filter, layer);
        assert values != null;

        attributeLoop:
        for (Attribute a : attributes) {
            int[] containedColumns = a.getContainedColumns();

            for (int column : containedColumns) {
                if (values[column] == null) {
                    a.setCurrentValue(null);
                    continue attributeLoop;
                }
            }
            a.setCurrentValue(buildCurrentValue(values, containedColumns));
        }
    }

    private String buildCurrentValue(String[] values, int[] containedColumns) {
        StringBuilder entry = new StringBuilder();
        // encode lengths to ensure uniqueness for multiple columns
        if (containedColumns.length > 1) {
            for (int i = 0; i < containedColumns.length - 2; i++) {
                entry.append(values[containedColumns[i]].length()).append(':');
            }
            entry.append(values[containedColumns[containedColumns.length - 2]].length()).append('|');
        }

        for (int containedColumn : containedColumns) {
            entry.append(values[containedColumn]);
        }

        return entry.toString();
    }


    /**
     * Checks if there is a next line in the input file
     *
     * @return true if another row is present false otherwise
     */
    public boolean hasNext() {
        return !(this.nextLine == null);
    }

    private String[] next(BloomFilter<Long> filter, int layer) {
        String[] currentLine = this.nextLine;

        this.nextLine = readNextLine();

        if (!chunkReader && config.inputFileSkipDifferingLines) {
            readToNextValidLine();
        }

        if (layer > 1) {
            replaceNonInformative(currentLine, filter);
        }

        currentLineNumber++;
        return currentLine;
    }

    private void replaceNonInformative(String[] currentLine, BloomFilter<Long> filter) {
        for (int i = 0; i < currentLine.length; i++) {
            if (currentLine[i] != null && !filter.mightContain(Hashing.hash(currentLine[i]))) {
                currentLine[i] = null;
            }
        }
    }

    /**
     * Reads the next input line and stores it in a String array.
     *
     * @return The values of the next line
     */
    public String[] next() {
        return next(null, 1);
    }

    /**
     * If there should be a line which does not have the expected number of entries, this method skips as many lines as
     * necessary until the next valid line is reached or the input ends.
     */
    private void readToNextValidLine() {
        if (!hasNext()) return;

        while (hasNext() && this.nextLine.length != this.headerLine.length) {
            this.nextLine = readNextLine();
            this.numberOfSkippedLines++;
        }
    }

    /**
     * Generates a pseudo headline, if a file does not have a header itself. Uses the DEFAULT_HEADER as a prefix and the
     * column number to generate names.
     */
    private void generateHeaderLine() {
        headerLine = new String[nextLine.length];
        for (int i = 0; i < this.nextLine.length; i++) {
            headerLine[i] = config.DEFAULT_HEADER_STRING + i;
        }
    }

    /**
     * Reads the next line of the input file
     *
     * @return null if there was no next line else the array of values
     */
    private String[] readNextLine() {
        String[] lineArray = null;
        try {
            lineArray = this.CSVReader.readNext();
        } catch (CsvValidationException | IOException e) {
            e.printStackTrace();
        }
        if (lineArray == null) {
            return null;
        }
        replaceNullAndEscape(lineArray);
        return lineArray;
    }

    /**
     * Replaces every string which resembles null to an actual null value
     *
     * @param lineArray The array of values to be processed
     */
    private void replaceNullAndEscape(String[] lineArray) {
        for (int i = 0; i < lineArray.length; i++) {
            if (chunkReader && lineArray[i].equals(config.nullString)) {
                if (config.nullHandling != Config.NullHandling.EQUALITY) {
                    // in equality mode, we treat every null entry as the same exact value
                    lineArray[i] = null;
                }
            } else if (!chunkReader) {
                lineArray[i] = lineArray[i].replace('\n', '\0');
            }
        }
    }

    public void close() throws IOException {
        CSVReader.close();
    }

}