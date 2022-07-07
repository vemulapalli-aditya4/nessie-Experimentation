import java.io.Serializable;
import java.util.List;

public class DeltaLakeTableInfo implements Serializable {

    public String id;
    public List<String> metadataLocationHistory;
    public List<String> checkpointLocationHistory;
    public String lastCheckpoint;

    public DeltaLakeTableInfo(String id, List<String> metadataLocationHistory, List<String> checkpointLocationHistory, String lastCheckpoint) {
        this.id = id;
        this.metadataLocationHistory = metadataLocationHistory;
        this.checkpointLocationHistory = checkpointLocationHistory;
        this.lastCheckpoint = lastCheckpoint;
    }
}
