package structures;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayDeque;

/**
 * A ValidationReader is attached to a single fully sorted relation during validation. The internal queue buffers the head values reducing the sorting efforts.
 */
public class ValidationReader {
    private final BufferedReader reader;
    private final int size;
    public ArrayDeque<Entry> queue;
    public boolean finished;

    /**
     * @param relationPath the path of the sorted relation file.
     * @param queueSize    the number of (sorted) values to buffer
     * @throws FileNotFoundException if the relation file could not be accessed.
     */
    public ValidationReader(String relationPath, int queueSize) throws FileNotFoundException {
        this.reader = new BufferedReader(new FileReader(relationPath));
        this.queue = new ArrayDeque<>(queueSize); // we know exactly how much the queue can grow
        this.size = queueSize;
        finished = false;
    }

    /**
     * The central method of the ValidationReader. It will refill the internal queue and return the biggest value stored within the queue.
     *
     * @return The biggest value of the queue.
     */
    public String update() {

        if (finished || queue.size() == size) {
            return queue.getLast().getValue();
        }

        String nextLine = null;
        try {
            // load the next values until the queue has been refilled or the input ran out.
            while (queue.size() < size && (nextLine = reader.readLine()) != null) {
                // do not deserialize the connected attributes yet.
                queue.add(new Entry(nextLine, reader.readLine()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // if the relation file ran out of values, we mark the relation as finished.
        if (nextLine == null) {
            finished = true;
        }

        // O(1) since we use a ArrayDequeue
        return queue.getLast().getValue();
    }

    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
