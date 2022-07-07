import java.io.Serializable;

public class ReferenceInfoExport  implements Serializable {

    public String referenceName;

    /**Type can be "branch" or "tag" */
    public String type;

    public String hash;

    public ReferenceInfoExport(String referenceName, String type, String hash)
    {
        this.type = type;
        this.referenceName = referenceName;
        this.hash = hash;
    }
}
