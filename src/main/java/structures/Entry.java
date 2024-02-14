package structures;

import lombok.Getter;
import lombok.Setter;

/**
 * The entry class handles a single entry of some attribute. It provides comparison methods.
 */
@Getter
@Setter
public
class Entry implements Comparable<Entry> {

    private final int readerNumber;
    private String value;
    private long occurrence;

    /**
     * To initialize an Entry we need its value, the number of occurrences and a pointer to the reader, that read the
     * entry.
     *
     * @param value        The sting representation of the value, that the entry carries.
     * @param occurrence   The number of occurrences within the attribute.
     * @param readerNumber The id of the reader, that read the value.
     */
    public Entry(final String value, long occurrence, final int readerNumber) {
        this.value = value;
        this.occurrence = occurrence;
        this.readerNumber = readerNumber;
    }

    @Override
    public int compareTo(Entry other) {
        return this.value.compareTo(other.value);
    }

    @Override
    public int hashCode() {
        return this.value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Entry other)) {
            return false;
        }
        return this.value.equals(other.value);
    }

    @Override
    public String toString() {
        return "Tuple(" + this.value + "," + this.occurrence + ", " + this.readerNumber + ")";
    }
}