package metanome;

import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import io.RowInput;

import java.io.IOException;

final class MetanomeInput implements RowInput {
    private final RelationalInput input;
    private boolean closed;

    MetanomeInput(RelationalInput input) {
        this.input = input;
    }

    @Override
    public String[] columnNames() {
        return input.columnNames().toArray(String[]::new);
    }

    @Override
    public boolean hasNext() throws IOException {
        if (Thread.currentThread().isInterrupted()) throw new IOException("Input reading interrupted");
        try {
            return input.hasNext();
        } catch (InputIterationException e) {
            throw new IOException("Could not read " + input.relationName(), e);
        }
    }

    @Override
    public String[] next() throws IOException {
        try {
            return input.next().toArray(String[]::new);
        } catch (InputIterationException e) {
            throw new IOException("Could not read " + input.relationName(), e);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
            input.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Input closing interrupted", e);
        } catch (Exception e) {
            throw new IOException("Could not close " + input.relationName(), e);
        }
    }
}
