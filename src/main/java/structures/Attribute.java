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
    Metadata metadata;

    public Attribute(int id, int relationId, int[] containedColumns) {
        this.id = id;
        this.relationId = relationId;
        this.containedColumns = containedColumns;
        this.metadata = new Metadata();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Attribute attribute = (Attribute) o;
        return relationId == attribute.relationId && Arrays.equals(containedColumns, attribute.containedColumns);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(relationId);
        result = 31 * result + Arrays.hashCode(containedColumns);
        return result;
    }
}
