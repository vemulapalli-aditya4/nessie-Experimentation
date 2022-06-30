import com.google.protobuf.ByteString;
import org.junit.Test;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.persist.adapter.*;
import org.projectnessie.versioned.persist.mongodb.ImmutableMongoClientConfig;
import org.projectnessie.versioned.persist.mongodb.MongoClientConfig;
import org.projectnessie.versioned.persist.mongodb.MongoDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.mongodb.MongoDatabaseClient;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class TestCommitLogEntry {

    @Test
    public void TestCommitLogEntries() throws ReferenceNotFoundException {
        MongoClientConfig mongoClientConfig = ImmutableMongoClientConfig.builder()
                .connectionString("mongodb://root:password@localhost:27017").databaseName("nessie").build();

        MongoDatabaseClient MongoDBClient = new MongoDatabaseClient();
        MongoDBClient.configure(mongoClientConfig);
        MongoDBClient.initialize();
        System.out.println("Mongo DB Client Initialized");

        System.out.println("Count of reflog is " + MongoDBClient.getRefLog().countDocuments());
        System.out.println("Count of commitlog is  " + MongoDBClient.getCommitLog().countDocuments());

        long count =  MongoDBClient.getCommitLog().countDocuments();
        StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();

        DatabaseAdapter mongoDatabaseAdapter = new MongoDatabaseAdapterFactory()
                .newBuilder()
                .withConnector(MongoDBClient)
                .withConfig(ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder().build())
                .build(storeWorker);
        System.out.println("DatabaseAdapter Initialized");


        Stream<CommitLogEntry> commitLogTable = mongoDatabaseAdapter.scanAllCommitLogEntries();

        // long count = commitLogTable.count();
        // System.out.println(" " + count + "\n");

        // First Commit Log Entry
        // CommitLogEntry commitLogEntry = commitLogTable.findFirst().orElse(null);

        //Last commit Log Entry
        CommitLogEntry commitLogEntry = commitLogTable.skip(count - 1).findFirst().orElse(null);

        Hash hash = commitLogEntry.getHash();
        String hashAsString = hash.asString();
        System.out.println("Commit Log Entry Hash is " +  hashAsString);

        long createdTime = commitLogEntry.getCreatedTime();
        System.out.println("Commit Log Created Time is " + createdTime);

        long getCommitSeq = commitLogEntry.getCommitSeq();
        System.out.println("Commit Log Entry commit sequence is  " + getCommitSeq);

        List<Hash> getParents = commitLogEntry.getParents();
        int size = getParents.size();
        System.out.println("Number of parents of this commit log entry - " + size);
        for ( int i = 0 ; i < size ; i++)
        {
            System.out.println("Parent is " + getParents.get(i).asString());
        }

        /** Isn't this already Serialized commit-metadata. ?*/
        /**What does this ""metadata (serialized via StoreWorker)"" mean ?*/
        ByteString getMetadata = commitLogEntry.getMetadata();


        List<KeyWithBytes> getPuts = commitLogEntry.getPuts();
        for( int i = 0 ; i < getPuts.size() ; i++)
        {
            // System.out.println(i);
            ContentId contentId = getPuts.get(i).getContentId();
            System.out.println("Puts Content id is " + contentId.toString());

            /** Table Name */
            Key key = getPuts.get(i).getKey();
            System.out.println("Puts Key is " + key.toString());

            byte type = getPuts.get(i).getType();
            System.out.println("Type is " +type);
            /** I think here type meant REF Type
             * enum RefType {
             *   Branch = 0;
             *   Tag = 1;
             * }
             */

            ByteString getValue = getPuts.get(i).getValue();
            /** Is this the Content ?
             * What does
             * ""use org.projectnessie.versioned.StoreWorker#valueFromStore to serialize as an instance of Content
             * D.
             * if valueFromStore calls the Supplier, you can get the global value via
             * org.projectnessie.versioned.persist.adapter.DatabaseAdapter#globalContent
             * puts - the values serialized via StoreWorker"" ??
             * mean */

        }

        List<Hash> additionalParents = commitLogEntry.getAdditionalParents();
        System.out.println("Number of additional parents of this commit log entry - " + additionalParents.size());
        for ( int i = 0 ; i < additionalParents.size() ; i++)
        {
            Hash x = additionalParents.get(i);
            System.out.println("Additional Parent is " + x.asString());
        }

        List<Key> deletes = commitLogEntry.getDeletes();
        for( int i = 0 ; i < deletes.size() ; ++i)
        {
            System.out.println("Deletes Key is " + deletes.get(i).toString());
        }

        KeyList keyList = commitLogEntry.getKeyList();
        /**public interface KeyList {
            List<KeyListEntry> getKeys();*/
        /** public interface KeyListEntry {
            Key getKey();

            ContentId getContentId();

            byte getType();

            @Nullable
            Hash getCommitId();*/

        /** Doubt */
        /** Handling Null Pointer Exception */
        List<KeyListEntry> key_list = (keyList != null ? keyList.getKeys() : null);
        if( key_list != null)
        {
            for(int i = 0 ; i < key_list.size(); i++)
            {
                System.out.println("i = " + i);
                System.out.println("KeyListEntry content Id is " + key_list.get(i).getContentId().toString());
                System.out.println("KeyListEntry type  is " + key_list.get(i).getType());
                System.out.println("KeyListEntry key  is " + key_list.get(i).getKey());
                /** Handling Null Pointer Exception */
                System.out.println("KeyListEntry commit ID  is " + Objects.requireNonNull(key_list.get(i).getCommitId()).asString());
            }
        }

        /**I think this is the overflow to store keys if not fitting in the KeyList keyList*/
        List<Hash> keyListsIds = commitLogEntry.getKeyListsIds();
        /** fetchKeyLists is used to get the KeyList Entity by using KeyListIds we got using above func
        Stream<KeyListEntity> fetchKeyLists = mongoDatabaseAdapter.fetchKeyLists(ctx, keyListsIds);
        public interface KeyListEntity {
            Hash getId();

            KeyList getKeys();

        public interface KeyList {
            List<KeyListEntry> getKeys(); */

        // refs.map(r -> r.getNamedRef().getName()).forEach(System.out::println);
    }


}
