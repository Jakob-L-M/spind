package structures;

import lombok.Getter;

import java.util.HashMap;

/**
 * The entry class handles a single entry of some attribute. It provides comparison methods.
 */
@Getter
public
class Entry implements Comparable<Entry> {

    private final int readerNumber;
    private final String value;
    private final String serializedAttributes;
    private HashMap<Integer, Long> connectedAttributes;


    /**
     * Merge Constructor
     *
     * @param value                The entries string value.
     * @param serializedAttributes The serialized string representation of the connected attributes. Id1,Occurrences1;Id2,Occurrences2;...
     * @param readerNumber         The reader, that the entry is attached to.
     */
    public Entry(final String value, String serializedAttributes, final int readerNumber) {
        this.value = value;
        this.readerNumber = readerNumber;
        this.serializedAttributes = serializedAttributes;
    }

    /**
     * Validation constructor.
     *
     * @param value                The entries string value.
     * @param serializedAttributes The serialized string representation of the connected attributes. Id1,Occurrences1;Id2,Occurrences2;...
     */
    public Entry(final String value, String serializedAttributes) {
        this.value = value;
        this.readerNumber = 0;
        this.serializedAttributes = serializedAttributes;
    }

    /**
     * Loads the serialized attributes in to the connected attributes hash map
     */
    public void load() {
        buildAttributeMap(serializedAttributes);
    }

    private void buildAttributeMap(String part) {
        connectedAttributes = new HashMap<>();
        String[] attributes = part.split(";");
        for (String attribute : attributes) {
            String[] idOccurrenceTuple = attribute.split(",");
            if (idOccurrenceTuple.length != 2) {
                continue;
            }
            connectedAttributes.put(Integer.valueOf(idOccurrenceTuple[0]), Long.valueOf(idOccurrenceTuple[1]));
        }
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
        return "Tuple(" + this.value + "," + this.readerNumber + ")";
    }
}