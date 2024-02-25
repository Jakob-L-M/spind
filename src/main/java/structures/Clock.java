package structures;

import java.util.HashMap;

public class Clock {

    HashMap<String, Long> clocks;
    HashMap<String, Long> measures;

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
        return measures.compute(identifier, (k, v) -> v == null ? timeToAdd : v + timeToAdd);
    }
}
