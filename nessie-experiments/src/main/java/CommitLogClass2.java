import org.projectnessie.model.Content;

import java.io.Serializable;
import java.util.List;

public class CommitLogClass2 implements Serializable {

    public List<Content> contents;

    public CommitLogClass2(List<Content> contents) {
        this.contents = contents;
    }
}
