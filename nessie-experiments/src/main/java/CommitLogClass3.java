import org.projectnessie.model.CommitMeta;

import java.io.Serializable;

public class CommitLogClass3 implements Serializable {

    public CommitMeta metaData;

    public CommitLogClass3(CommitMeta metaData) {
        this.metaData = metaData;
    }
}
