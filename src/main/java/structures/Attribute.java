package structures;

import lombok.Data;

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
}
