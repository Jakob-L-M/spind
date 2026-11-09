package io;

import structures.Attribute;
import structures.Entry;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Class to handel file merging.
 */
public class Merger {
    private PriorityQueue<Entry> headValues;
    private BufferedReader[] readers;

    /**
     * Attaches the readers and load the first value.
     *
     * @param files The List of paths to files which should be merged.
     * @throws IOException if a files could not be found.
     */
    private void init(List<Path> files) throws IOException {
        this.headValues = new PriorityQueue<>(files.size());
        this.readers = new BufferedReader[files.size()];

        for (int index = 0; index < files.size(); ++index) {
            BufferedReader reader = Files.newBufferedReader(files.get(index));
            this.readers[index] = reader;
            String firstLine = reader.readLine();
            if (firstLine != null) {
                this.headValues.add(new Entry(firstLine, reader.readLine(), index));
            }
        }

    }

    public void merge(List<Path> files, Path to, Attribute[] attributes, boolean isFinal) throws IOException {
        try {
            this.init(files);

            try (BufferedWriter output = Files.newBufferedWriter(to, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                if (!headValues.isEmpty()) {
                    Entry previous = headValues.poll();
                    updateHeadValues(previous);
                    HashMap<Integer, Long> containedAttributes = new HashMap<>();

                    while (!this.headValues.isEmpty()) {
                        if (Thread.currentThread().isInterrupted()) throw new IOException("Merging interrupted");
                        Entry current = headValues.poll();
                        if (!previous.equals(current)) {
                            // The previous value group is complete; save it to disk.
                            writeValue(containedAttributes, attributes, output, previous, isFinal);
                            if (!containedAttributes.isEmpty()) containedAttributes = new HashMap<>();
                        } else {
                            // Add the previous entry to the current value group.
                            previous.load(containedAttributes);
                        }
                        previous = current;
                        updateHeadValues(current);
                    }
                    writeValue(containedAttributes, attributes, output, previous, isFinal);
                }
            }
        } finally {
            closeReaders();
        }
        for (Path path : files) {
            Files.delete(path);
        }
    }

    /**
     * Updates the reader which is connected to the current entry.
     *
     * @param current the entry which was last handled
     * @throws IOException If the reader is unable to read the next two lines
     */
    private void updateHeadValues(Entry current) throws IOException {
        int readerNumber = current.getReaderNumber();
        String nextLine = this.readers[readerNumber].readLine();

        if (nextLine != null) {
            // if there is one more line, there have to be two more lines.
            this.headValues.add(new Entry(nextLine, this.readers[readerNumber].readLine(), readerNumber));
        }
    }

    private void writeValue(HashMap<Integer, Long> containedAttributes, Attribute[] attributes, BufferedWriter output, Entry previous, boolean isFinal) throws IOException {
        output.write(previous.getValue());
        output.newLine(); // delimiter between value and connected attributes

        // Case 1: There is only
        if (containedAttributes.isEmpty()) {
            output.write(previous.getSerializedAttributes());
            if (isFinal) {
                previous.load();
                for (Integer attribute : previous.getConnectedAttributes().keySet()) {
                    long occurrences = previous.getConnectedAttributes().get(attribute);
                    attributes[attribute].getMetadata().totalValues += occurrences;
                    attributes[attribute].getMetadata().uniqueValues++;
                }
            }
        } else {
            // Case 2: there are multiple attributes from multiple files in the group
            previous.load(containedAttributes); // add the last member
            for (Map.Entry<Integer, Long> attribute : containedAttributes.entrySet()) {

                writeAttribute(output, attribute.getKey(), attribute.getValue());

                if (isFinal) {
                    attributes[attribute.getKey()].getMetadata().totalValues += attribute.getValue();
                    attributes[attribute.getKey()].getMetadata().uniqueValues++;
                }
            }
        }

        output.newLine();
    }

    private void writeAttribute(BufferedWriter output, int key, long occurrences) throws IOException {
        output.write(String.valueOf(key));
        output.write(','); // attribute-occurrence separator
        output.write(String.valueOf(occurrences));
        output.write(';'); // attribute-attribute separator
    }

    /**
     * Closes (and flushes) all readers.
     *
     * @throws IOException If a file is locked and the reader therefore can not be closed.
     */
    private void closeReaders() throws IOException {
        if (readers == null) return;
        IOException failure = null;
        for (BufferedReader reader : readers) {
            if (reader == null) continue;
            try {
                reader.close();
            } catch (IOException e) {
                if (failure == null) failure = e;
                else failure.addSuppressed(e);
            }
        }
        if (failure != null) throw failure;
    }
}
