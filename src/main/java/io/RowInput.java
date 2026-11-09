package io;

import java.io.IOException;

/** A streaming table input used by the chunking stage. */
public interface RowInput extends AutoCloseable {
    String[] columnNames();

    boolean hasNext() throws IOException;

    String[] next() throws IOException;

    @Override
    void close() throws IOException;
}
