package io;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import runner.Config;
import structures.Attribute;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class RelationalInput {

    private final List<Attribute> attributes;
    public String[] headerLine;
    public int relationOffset;
    protected CSVReader CSVReader;
    protected String[] nextLine;
    protected String relationName;
    protected int numberOfColumns = 0;
    // Initialized to -1 because of lookahead
    protected int currentLineNumber = -1;
    protected int numberOfSkippedLines = 0;

    private final Config config;


    public RelationalInput(String relationName, String relationPath, Config config, int relationOffset, List<Attribute> attributes) throws IOException {
        this.relationName = relationName;
        this.relationOffset = relationOffset;
        this.attributes = attributes;

        this.config = config;

        BufferedReader reader = new BufferedReader(new FileReader(relationPath));

        this.CSVReader = new CSVReaderBuilder(reader)
                .withCSVParser(
                        new CSVParserBuilder()
                                .withSeparator(config.separator)
                                .withEscapeChar(config.fileEscape)
                                .withIgnoreLeadingWhiteSpace(config.ignoreLeadingWhiteSpace)
                                .withStrictQuotes(config.strictQuotes)
                                .withQuoteChar(config.quoteChar)
                                .build()
                ).build();

        // read the first line
        this.nextLine = readNextLine();
        if (this.nextLine != null) {
            this.numberOfColumns = this.nextLine.length;
        }

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
     * Builds the string representation for every attribute combination of the given table and updates the attributes
     * accordingly.
     */
    public void updateAttributeCombinations() throws IOException {
        String[] values = next();

        for (Attribute a : attributes) {
            StringBuilder entry = new StringBuilder();
            int[] containedColumns = a.getContainedColumns();

            // encode lengths to ensure uniqueness for multiple columns
            if (containedColumns.length > 1) {
                for (int i = 0; i < containedColumns.length - 2; i++) {
                    entry.append(values[i].length()).append(':');
                }
                entry.append(values[containedColumns[containedColumns.length - 2]].length()).append('|');
            }

            for (int containedColumn : containedColumns) {
                entry.append(values[containedColumn]);
            }

            a.setCurrentValue(entry.toString());
        }
    }

    /**
     * Checks if there is a next line in the input file
     * @return true if another row is present false otherwise
     */
    public boolean hasNext() {
        return !(this.nextLine == null);
    }

    /**
     * Reads the next input line and stores it in a String array.
     * @return The values of the next line
     * @throws IOException
     */
    public String[] next() throws IOException {
        if (!hasNext()) return null;

        String[] currentLine = this.nextLine;

        if (currentLine == null) {
            return null;
        }
        this.nextLine = readNextLine();

        if (config.inputFileSkipDifferingLines) {
            readToNextValidLine();
        } else {
            failDifferingLine(currentLine);
        }
        currentLineNumber++;
        return currentLine;
    }

    /**
     * If skipping a line with unexpected entries is not an option, this method will throw an error.
     * @param currentLine the line to be checked
     * @throws IOException if the size of the line does not match the expected size
     */
    private void failDifferingLine(String[] currentLine) throws IOException {
        if (currentLine.length != this.numberOfColumns) {
            throw new IOException("Unexpected number of entries encountered in table: " + relationName + " for row: " + this.currentLineNumber);
        }
    }

    /**
     * If there should be a line which does not have the expected number of entries, this method skips as many lines as
     * necessary until the next valid line is reached or the input ends.
     */
    private void readToNextValidLine() {
        if (!hasNext()) return;

        while (hasNext() && this.nextLine.length != this.numberOfColumns) {
            this.nextLine = readNextLine();
            this.numberOfSkippedLines++;
        }
    }

    /**
     * Generates a pseudo headline, if a file does not have a header itself. Uses the DEFAULT_HEADER as a prefix and the
     * column number to generate names.
     */
    private void generateHeaderLine() {
        headerLine = new String[numberOfColumns];
        for (int i = 0; i < this.numberOfColumns; i++) {
            headerLine[i] = config.DEFAULT_HEADER_STRING + i;
        }
    }

    /**
     * Reads the next line of the input file
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
        replaceNull(lineArray);
        return lineArray;
    }

    /**
     * Replaces every string which resembles null to an actual null value
     * @param lineArray The array of values to be processed
     */
    private void replaceNull(String[] lineArray) {
        for (int i = 0; i < lineArray.length; i++) {
            if (lineArray[i].equals(config.inputFileNullString)) {
                lineArray[i] = null;
            }
        }
    }

    public void close() throws IOException {
        CSVReader.close();
    }

}