package structures;

import lombok.Data;

import java.util.Arrays;
import java.util.Objects;

@Data
public class Attribute {
    int id;
    int relationId;
    int[] containedColumns;
    String currentValue;
    long currentOccurrences;
    PINDList referenced;
    Metadata metadata;

    public Attribute(int id, int relationId, int[] containedColumns) {
        this.id = id;
        this.relationId = relationId;
        this.containedColumns = containedColumns;
        this.metadata = new Metadata();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        return this.toString().equals(o.toString());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(relationId);
        result = 31 * result + Arrays.hashCode(containedColumns);
        return result;
    }

    @Override
    public String toString() {
        return "" + relationId + ": " + Arrays.toString(containedColumns);
    }
}
