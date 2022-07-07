import java.io.Serializable;

public class IcebergViewInfo implements Serializable {

    public String id;
    public String metadataLocation;
    public int versionId;
    public int schemaId;
    public String sqlText;
    public String dialect;

    public IcebergViewInfo(String id, String metadataLocation, int versionId, int schemaId, String sqlText, String dialect) {
        this.id = id;
        this.metadataLocation = metadataLocation;
        this.versionId = versionId;
        this.schemaId = schemaId;
        this.sqlText = sqlText;
        this.dialect = dialect;
    }
}
