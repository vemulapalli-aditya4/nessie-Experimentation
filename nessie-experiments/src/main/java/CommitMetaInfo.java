import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

public class CommitMetaInfo implements Serializable {

    public String author;

    public Instant commitTime;

    public Instant authorTime;

    public String hash;

    public String committer;

    public String message;

    public Map<String, String> properties;

    public String signedOffBy;

    public CommitMetaInfo(String author, Instant commitTime, Instant authorTime,
                          String hash, String committer, String message,
                          Map<String, String> properties, String signedOffBy)
    {
        this.author = author;
        this.commitTime = commitTime;
        this.authorTime = authorTime;
        this.hash = hash;
        this.committer = committer;
        this.message = message;
        this.properties = properties;
        this.signedOffBy = signedOffBy;
    }

}