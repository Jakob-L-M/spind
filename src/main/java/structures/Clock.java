package structures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Clock {

    public final Map<String, List<Long>> measures;
    HashMap<String, Long> clocks;

    public Clock() {
        clocks = new HashMap<>();
        measures = new HashMap<>();
    }

    /**
     * Starts a specific clock
     *
     * @param identifier the clock to start
     */
    public void start(String identifier) {
        clocks.put(identifier, System.currentTimeMillis());
    }

    /**
     * Adds the time of a clock and stops the clock
     *
     * @param identifier the clock to stop.
     * @return the total time of the stopped clock.
     */
    public long stop(String identifier) {
        long timeToAdd = System.currentTimeMillis() - clocks.remove(identifier);
        List<Long> measure = measures.computeIfAbsent(identifier, k -> new ArrayList<>());
        measure.add(timeToAdd);
        return timeToAdd;
    }
}
