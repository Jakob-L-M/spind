package structures;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;

public class ValidationReader {
    private BufferedReader reader;
    private int size;
    public ArrayDeque<Entry> queue;


    public ValidationReader(Path relationPath,int bufferSize, int queueSize) throws FileNotFoundException {
        this.reader = new BufferedReader(new FileReader(relationPath.toString()), bufferSize);
        this.queue = new ArrayDeque<>(queueSize);
        this.size = queueSize;
    }

    public String update() throws IOException {
        String nextLine;
        while (queue.size() < size && (nextLine = reader.readLine()) != null) {
            queue.add(new Entry(nextLine, reader.readLine(), 0));
        }
        return queue.getLast().getValue();
    }
}
