package structures;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;

public class ValidationReader {
    private final BufferedReader reader;
    private final int size;
    public ArrayDeque<Entry> queue;
    public boolean finished;


    public ValidationReader(String relationPath,int bufferSize, int queueSize) throws FileNotFoundException {
        this.reader = new BufferedReader(new FileReader(relationPath), bufferSize);
        this.queue = new ArrayDeque<>(queueSize);
        this.size = queueSize;
        finished = false;
    }

    public String update() {

        if (queue.size() == size) {
            return queue.getLast().getValue();
        }

        String nextLine = null;
        try {
            while (queue.size() < size && (nextLine = reader.readLine()) != null) {
                queue.add(new Entry(nextLine, reader.readLine(), 0));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (nextLine == null) {
            finished = true;
        }

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
