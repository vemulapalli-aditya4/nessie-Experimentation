import java.io.Serializable;

public class IcebergTableInfo implements Serializable {
    public String id;
    public String metadataLocation;
    public long snapshotId;
    public int schemaId;
    public int specId;
    public int sortOrderId;

    public IcebergTableInfo(String id, String metadataLocation, long snapshotId, int schemaId, int specId, int sortOrderId) {
        this.id = id;
        this.metadataLocation = metadataLocation;
        this.snapshotId = snapshotId;
        this.schemaId = schemaId;
        this.specId = specId;
        this.sortOrderId = sortOrderId;
    }
}
