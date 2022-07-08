import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.persist.adapter.ContentId;

import java.util.List;

public class CommitLogClass {

    public long createdTime;

    public String hash;

    public long commitSeq;

    public CommitLogClass() {
    }

    public String parent_1st;

    public CommitMeta commitMeta;

    public List<ContentId> contentIds;

    public List<Content> contents;

    public List<Key> putsKeys;

    public List<Key> deletes;

    public List<String> additionalParents;

    public CommitLogClass(long createdTime, String hash, long commitSeq, String parent_1st, CommitMeta commitMeta,
                          List<ContentId> contentIds, List<Content> contents, List<Key> putsKeys, List<Key> deletes,
                          List<String> additionalParents) {
        this.createdTime = createdTime;
        this.hash = hash;
        this.commitSeq = commitSeq;
        this.parent_1st = parent_1st;
        this.commitMeta = commitMeta;
        this.contentIds = contentIds;
        this.contents = contents;
        this.putsKeys = putsKeys;
        this.deletes = deletes;
        this.additionalParents = additionalParents;
    }
}
