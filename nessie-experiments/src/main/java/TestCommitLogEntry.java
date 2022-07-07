//import com.google.gson.Gson;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonStreamParser;
//import com.google.gson.reflect.TypeToken;
//import com.google.protobuf.ByteString;
//import org.junit.Test;
//import org.projectnessie.model.CommitMeta;
//import org.projectnessie.model.Content;
//import org.projectnessie.server.store.TableCommitMetaStoreWorker;
//import org.projectnessie.versioned.*;
//import org.projectnessie.versioned.persist.adapter.*;
//import org.projectnessie.versioned.persist.mongodb.ImmutableMongoClientConfig;
//import org.projectnessie.versioned.persist.mongodb.MongoClientConfig;
//import org.projectnessie.versioned.persist.mongodb.MongoDatabaseAdapterFactory;
//import org.projectnessie.versioned.persist.mongodb.MongoDatabaseClient;
//import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;
//import org.projectnessie.versioned.persist.serialize.AdapterTypes;
//
//import javax.annotation.Nullable;
//import java.io.*;
//import java.lang.reflect.Type;
//import java.nio.ByteBuffer;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Objects;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
//import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.*;
//
//public class TestCommitLogEntry {
//
//    @Test
//    public void TestCommitLogEntries() throws ReferenceNotFoundException, RefLogNotFoundException {
//        MongoClientConfig mongoClientConfig = ImmutableMongoClientConfig.builder()
//                .connectionString("mongodb://root:password@localhost:27017").databaseName("nessie").build();
//
//        MongoDatabaseClient MongoDBClient = new MongoDatabaseClient();
//        MongoDBClient.configure(mongoClientConfig);
//        MongoDBClient.initialize();
//        System.out.println("Mongo DB Client Initialized");
//
//        System.out.println("Count of reflog is " + MongoDBClient.getRefLog().countDocuments());
//        System.out.println("Count of commitlog is  " + MongoDBClient.getCommitLog().countDocuments());
//
//        long count =  MongoDBClient.getCommitLog().countDocuments();
//        StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();
//
//        DatabaseAdapter mongoDatabaseAdapter = new MongoDatabaseAdapterFactory()
//                .newBuilder()
//                .withConnector(MongoDBClient)
//                .withConfig(ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder().build())
//                .build(storeWorker);
//        System.out.println("DatabaseAdapter Initialized");
//
//        Stream<RefLog> allReflogEntries = mongoDatabaseAdapter.refLog(null);
//
//         // System.out.println("Count of ref log when null is passed " + allReflogEntries.count());
//
//         // RefLog refLog = allReflogEntries.findFirst().orElse(null);
//
//         // System.out.println("Ref Log Entry commit Hash is " + refLog.getCommitHash().asString() );
//
////        /** Will the list be in the same order of stream ( is Stream an actual order of RefLogTable )  */
////        List<RefLog> refLogList = allReflogEntries.collect(Collectors.toList());
////
////        String refLogTableFilePath = "/Users/aditya.vemulapalli/Downloads/refLogTableProto";
////        List<AdapterTypes.RefLogEntry> refLogEntries = new ArrayList<AdapterTypes.RefLogEntry>();
////
////        boolean bl1 = false;
////        Hash h1 = null;
////        /** serialize the RefLog */
////        for (RefLog refLog : refLogList) {
////            AdapterTypes.RefLogEntry refLogEntry = toProtoFromRefLog(refLog);
////            refLogEntries.add(refLogEntry);
////            if(!bl1)
////            {
////                bl1 = true;
////                h1 = refLog.getCommitHash();
////            }
////        }
////
////        FileOutputStream fosRefLog = null;
////        FileInputStream fosRefLogInputStream = null;
////        try{
////             fosRefLog = new FileOutputStream(refLogTableFilePath);
////             fosRefLogInputStream = new FileInputStream(refLogTableFilePath);
////            int tLen = refLogEntries.size() * 4 ;
////            boolean bl = false;
////            byte[] bytes0Len;
////            RefLog rfLog = null;
////
////            for (AdapterTypes.RefLogEntry refLogEntry : refLogEntries) {
////                byte[] refLogEntryByteArray = refLogEntry.toByteArray();
////                int len = refLogEntryByteArray.length;
////                tLen += len;
////                ByteBuffer bb = ByteBuffer.allocate(4);
////                bb.putInt(len);
////                byte[] bytes = bb.array();
////                if(!bl)
////                {
////                   bl = true;
////                   bytes0Len = bytes;
////                   System.out.println("Ref log 0 commit hash without ser is " + h1 );
////                   rfLog = protoToRefLog(refLogEntryByteArray);
////                   System.out.println("ref Log 0 commit Hash is " + rfLog.getCommitHash().toString());
////
////                }
////                fosRefLog.write(bytes);
////                fosRefLog.write(refLogEntryByteArray);
////            }
////            Path path = Paths.get("/Users/aditya.vemulapalli/Downloads/refLogTableProto");
////            byte[] data = Files.readAllBytes(path);
////            System.out.println("length of total ref log table file is " + data.length);
////            System.out.println("Lenght of total ref log table file using tlen is " + tLen);
////
////
////        } catch( FileNotFoundException e ) {
////            throw new RuntimeException(e);
////        } catch (IOException e) {
////            e.printStackTrace();
////        } finally {
////            if(fosRefLog != null)
////            {
////                try{
////                    fosRefLog.close();
////                    fosRefLogInputStream.close();
////                } catch (IOException e) {
////                    e.printStackTrace();
////                }
////            }
////
////        }
////
////        System.out.println("Ref Log Table file is written");
//
//        /**************************************************************************************************/
//
////        RepoDescription repoDescTable = mongoDatabaseAdapter.fetchRepositoryDescription();
////        System.out.println("Repo Version using RepoDescription  is " + repoDescTable.getRepoVersion());
////
////        AdapterTypes.RepoProps repoProps = toProto(repoDescTable);
////
////        /***/
////        System.out.println("RepoProps proto is " + repoProps.toString());
////        System.out.println("RepoProps proto string length is " + repoProps.toString().length());
////
////        System.out.println("Repo Version using RepoProps is " + repoProps.getRepoVersion());
////
////        String repoDescFilePath = "/Users/aditya.vemulapalli/Downloads/repoDescProto";
////
////        byte[] arr = repoProps.toByteArray();
////
////        System.out.println("Repo props byte array length is " + arr.length);
////        FileOutputStream fosDescTable = null;
////        try{
////            fosDescTable = new FileOutputStream(repoDescFilePath);
////            // repoProps.writeTo(fosDescTable);
////            fosDescTable.write(arr);
////        } catch (IOException e) {
////            throw new RuntimeException(e);
////        } finally {
////            if(fosDescTable != null)
////            {
////                try {
////                    fosDescTable.close();
////                } catch (IOException e) {
////                    e.printStackTrace();
////                }
////            }
////        }
////        System.out.println("Repository Description file is written");
//
//        /**************************************************************************************************/
////        GetNamedRefsParams params = GetNamedRefsParams.DEFAULT;
////
////        Stream<ReferenceInfo<ByteString>> namedReferences = mongoDatabaseAdapter.namedRefs(params);
////
////        List<ReferenceInfo<ByteString>> namedReferencesList = namedReferences.collect(Collectors.toList());
////
////        List<Long> commitSeqList = new ArrayList<>();
////        List<Hash> hashList = new ArrayList<>();
////        List<ByteString> headCommitMeta = new ArrayList<>();
////        List<Hash> commonAncestorList = new ArrayList<>();
////        List<>
////        namedReferencesList.get(0).geT
//
//
//        /**************************************************************************************************/
////        GetNamedRefsParams params = GetNamedRefsParams.DEFAULT;
////
////        Stream<ReferenceInfo<ByteString>> namedReferences = mongoDatabaseAdapter.namedRefs(params);
////        List<ReferenceInfo<ByteString>> namedReferencesList = namedReferences.collect(Collectors.toList());
////        List<ImmutableReferenceInfo<ByteString>> immutableNamedRefsList = new ArrayList<ImmutableReferenceInfo<ByteString>>();
////        for(int i = 0 ; i < namedReferencesList.size(); i++)
////        {
////            ImmutableReferenceInfo<ByteString>  bs = (ImmutableReferenceInfo<ByteString>) namedReferencesList.get(i);
////            immutableNamedRefsList.add(bs);
////        }
//
////        String namedRefsTableFilePath = "/Users/aditya.vemulapalli/Downloads/namedRefs.json";
////        Writer writer = null;
////        Gson gson = new Gson();
////
////        /**Using GSON for serialization and de - serialization*/
////        /** Serialization is straight forward , deserialization must be done using custom deserializer */
////
////        /**Gson gson = new GsonBuilder().create(); --->for non readable format */
////
////        try{
////            writer = new FileWriter(namedRefsTableFilePath);
////
////            gson.toJson(namedReferencesList, writer);
//////            gson.toJson(immutableNamedRefsList, writer);
//////            for (ImmutableReferenceInfo<ByteString> byteStringImmutableReferenceInfo : immutableNamedRefsList) {
//////                gson.toJson(byteStringImmutableReferenceInfo, );
//////            }
////        } catch (IOException e) {
////            throw new RuntimeException(e);
////        } finally {
////            if(writer != null)
////            {
////                try {
////                    writer.close();
////                }
////                catch(IOException e){
////                    e.printStackTrace();
////                }
////            }
////        }
////        System.out.println("Named Refs is written");
//
////        Reader reader = null;
////
////        Gson gson2 = new Gson();
////
////        try{
////            reader = new FileReader(namedRefsTableFilePath);
////            JsonStreamParser parser = new JsonStreamParser(reader);
////            Type MyClassObject = new TypeToken<ReferenceInfo<ByteString>>() {}.getType();
////            while(parser.hasNext())
////            {
////                JsonElement e = parser.next();
////                if(e.isJsonObject())
////                {
////                    ReferenceInfo<ByteString> rfInfoBs = gson.fromJson(e, MyClassObject);
////                }
////            }
////
////        } catch (FileNotFoundException e) {
////            throw new RuntimeException(e);
////        }
//
//
////        Gson gson2 = new Gson();
////        Type listOfMyClassObject = new TypeToken<ArrayList<ImmutableReferenceInfo<ByteString>>>() {}.getType();
////        try {
////            List<ImmutableReferenceInfo<ByteString>> outputList = gson.fromJson(new FileReader(namedRefsTableFilePath), listOfMyClassObject);
////        } catch (FileNotFoundException e) {
////            throw new RuntimeException(e);
////        }
//
//
//        // User deserializedUser = gson.fromJson(new FileReader(filePath), User.class);
//
//        Stream<CommitLogEntry> commitLogTable = mongoDatabaseAdapter.scanAllCommitLogEntries();
//
//        // long count = commitLogTable.count();
//        // System.out.println(" " + count + "\n");
//
//        // First Commit Log Entry
//        // CommitLogEntry commitLogEntry = commitLogTable.findFirst().orElse(null);
//
//        //Last commit Log Entry
//        CommitLogEntry commitLogEntry = commitLogTable.skip(count - 1).findFirst().orElse(null);
//
////        byte[] commitLogEntryByteArr = new byte[0];
////        int l1 = 0;
////        if(commitLogEntry != null)
////        {
////            AdapterTypes.CommitLogEntry protoOriginal = toProto(commitLogEntry);
////
////            List<KeyWithBytes> newPuts  = new ArrayList<>();
////            newPuts = commitLogEntry.getPuts();
////            AdapterTypes.CommitLogEntry protoModified = AdapterTypes.CommitLogEntry.newBuilder()
////                    .mergeFrom(protoOriginal)
////                    .clearParents()
////                    .addParents(commitLogEntry.getParents().get(0).asBytes())
////                    .clearKeyListDistance()
////                    .clearKeyListIds()
////                    .clearKeyList()
////                    .clearPuts()
////                    .build();
////            /**.addAllPuts(newPuts)*/
////
////             commitLogEntryByteArr = protoModified.toByteArray();
////             l1 = l1 + 4 + commitLogEntryByteArr.length;
////            System.out.println("l1 is " + l1);
////            FileOutputStream fosCommitLog = null;
////            try{
////                fosCommitLog = new FileOutputStream("/Users/aditya.vemulapalli/Downloads/commitLogFile");
////
////                ByteBuffer bb = ByteBuffer.allocate(4);
////                bb.putInt(commitLogEntryByteArr.length);
////                byte[] lenOfCommitLogentry = bb.array();
////                fosCommitLog.write(lenOfCommitLogentry);
////                fosCommitLog.write(commitLogEntryByteArr);
////            } catch( FileNotFoundException e ) {
////                throw new RuntimeException(e);
////            } catch (IOException e) {
////                e.printStackTrace();
////            } finally {
////                if(fosCommitLog != null)
////                {
////                    try{
////                        fosCommitLog.close();
////                    } catch (IOException e) {
////                        e.printStackTrace();
////                    }
////                }
////
////            }
////        }
////
////        Path path = Paths.get("/Users/aditya.vemulapalli/Downloads/commitLogFile");
////        try {
////          CommitLogEntry cLogEntry = protoToCommitLogEntry(commitLogEntryByteArr);
////          byte[] fileBytes = Files.readAllBytes(path);
////          int l2 = fileBytes.length;
////            System.out.println("l2 is " + l2) ;
////          List<Hash> getParents = cLogEntry.getParents();
////          int size = getParents.size();
////          System.out.println("Number of parents of this commit log entry - " + size);
////          for ( int i = 0 ; i < size ; i++)
////          {
////                System.out.println("Parent is " + getParents.get(i).asString());
////          }
////
////          List<Hash> additionalParents = cLogEntry.getAdditionalParents();
////          System.out.println("Number of additional parents of this commit log entry - " + additionalParents.size());
////          for ( int i = 0 ; i < additionalParents.size() ; i++)
////          {
////                Hash x = additionalParents.get(i);
////                System.out.println("Additional Parent is " + x.asString());
////          }
////
////          List<Key> deletes = cLogEntry.getDeletes();
////          for( int i = 0 ; i < deletes.size() ; ++i)
////            {
////                System.out.println("Deletes Key is " + deletes.get(i).toString());
////            }
////
////          KeyList keyList = cLogEntry.getKeyList();
////
////            List<KeyListEntry> key_list = (keyList != null ? keyList.getKeys() : null);
////            if( key_list != null)
////            {
////                for(int i = 0 ; i < key_list.size(); i++)
////                {
////                    System.out.println("i = " + i);
////                    System.out.println("KeyListEntry content Id is " + key_list.get(i).getContentId().toString());
////                    System.out.println("KeyListEntry type  is " + key_list.get(i).getType());
////                    System.out.println("KeyListEntry key  is " + key_list.get(i).getKey());
////                    /** Handling Null Pointer Exception */
////                    System.out.println("KeyListEntry commit ID  is " + Objects.requireNonNull(key_list.get(i).getCommitId()).asString());
////                }
////            }
////
////            List<Hash> keyListsIds = cLogEntry.getKeyListsIds();
////            System.out.println("KeyListIds count is " + keyListsIds.size());
////            int value = cLogEntry.getKeyListDistance();
////
////            System.out.println("Key list distance is " + value);
////
////
////        } catch (Exception e) {
////            throw new RuntimeException(e);
////        }
//
//
//        Hash hash = commitLogEntry.getHash();
//        String hashAsString = hash.asString();
//        System.out.println("Commit Log Entry Hash is " +  hashAsString);
//
//        long createdTime = commitLogEntry.getCreatedTime();
//        System.out.println("Commit Log Created Time is " + createdTime);
//
//        long getCommitSeq = commitLogEntry.getCommitSeq();
//        System.out.println("Commit Log Entry commit sequence is  " + getCommitSeq);
//
//        List<Hash> getParents = commitLogEntry.getParents();
//        int size = getParents.size();
//        System.out.println("Number of parents of this commit log entry - " + size);
//        for ( int i = 0 ; i < size ; i++)
//        {
//            System.out.println("Parent is " + getParents.get(i).asString());
//        }
//
//        /** Isn't this already Serialized commit-metadata. ?*/
//        /**What does this ""metadata (serialized via StoreWorker)"" mean ?*/
//        ByteString getMetadata = commitLogEntry.getMetadata();
//
//
//        List<KeyWithBytes> getPuts = commitLogEntry.getPuts();
//        for( int i = 0 ; i < getPuts.size() ; i++)
//        {
//            // System.out.println(i);
//            ContentId contentId = getPuts.get(i).getContentId();
//            System.out.println("Puts Content id is " + contentId.toString());
//
//            /** Table Name */
//            Key key = getPuts.get(i).getKey();
//            System.out.println("Puts Key is " + key.toString());
//
//            byte type = getPuts.get(i).getType();
//            System.out.println("Type is " +type);
//            /** I think here type meant REF Type
//             * enum RefType {
//             *   Branch = 0;
//             *   Tag = 1;
//             * }
//             */
//
//            ByteString getValue = getPuts.get(i).getValue();
//            /** Is this the Content ?
//             * What does
//             * ""use org.projectnessie.versioned.StoreWorker#valueFromStore to serialize as an instance of Content
//             * D.
//             * if valueFromStore calls the Supplier, you can get the global value via
//             * org.projectnessie.versioned.persist.adapter.DatabaseAdapter#globalContent
//             * puts - the values serialized via StoreWorker"" ??
//             * mean */
//
//        }
//
//        List<Hash> additionalParents = commitLogEntry.getAdditionalParents();
//        System.out.println("Number of additional parents of this commit log entry - " + additionalParents.size());
//        for ( int i = 0 ; i < additionalParents.size() ; i++)
//        {
//            Hash x = additionalParents.get(i);
//            System.out.println("Additional Parent is " + x.asString());
//        }
//
//        List<Key> deletes = commitLogEntry.getDeletes();
//        for( int i = 0 ; i < deletes.size() ; ++i)
//        {
//            System.out.println("Deletes Key is " + deletes.get(i).toString());
//        }
//
//        KeyList keyList = commitLogEntry.getKeyList();
//        /**public interface KeyList {
//            List<KeyListEntry> getKeys();*/
//        /** public interface KeyListEntry {
//            Key getKey();
//
//            ContentId getContentId();
//
//            byte getType();
//
//            @Nullable
//            Hash getCommitId();*/
//
//        /** Doubt */
//        /** Handling Null Pointer Exception */
//        List<KeyListEntry> key_list = (keyList != null ? keyList.getKeys() : null);
//        if( key_list != null)
//        {
//            for(int i = 0 ; i < key_list.size(); i++)
//            {
//                System.out.println("i = " + i);
//                System.out.println("KeyListEntry content Id is " + key_list.get(i).getContentId().toString());
//                System.out.println("KeyListEntry type  is " + key_list.get(i).getType());
//                System.out.println("KeyListEntry key  is " + key_list.get(i).getKey());
//                /** Handling Null Pointer Exception */
//                System.out.println("KeyListEntry commit ID  is " + Objects.requireNonNull(key_list.get(i).getCommitId()).asString());
//            }
//        }
//
//        /**I think this is the overflow to store keys if not fitting in the KeyList keyList*/
//        List<Hash> keyListsIds = commitLogEntry.getKeyListsIds();
//        System.out.println("KeyListIds count is " + keyListsIds.size());
//        /** fetchKeyLists is used to get the KeyList Entity by using KeyListIds we got using above func
//        Stream<KeyListEntity> fetchKeyLists = mongoDatabaseAdapter.fetchKeyLists(ctx, keyListsIds);
//        public interface KeyListEntity {
//            Hash getId();
//
//            KeyList getKeys();
//
//        public interface KeyList {
//            List<KeyListEntry> getKeys(); */
//
//        int value = commitLogEntry.getKeyListDistance();
//
//        System.out.println("Key list distance is " + value);
//        // refs.map(r -> r.getNamedRef().getName()).forEach(System.out::println);
//    }
//
//    public AdapterTypes.RefLogEntry toProtoFromRefLog(RefLog refLog)
//    {
//        /** Reference type can be 'Branch' or 'Tag'. */
//        AdapterTypes.RefType refType = Objects.equals(refLog.getRefType(), "Tag") ? AdapterTypes.RefType.Tag : AdapterTypes.RefType.Branch;
//        /**enum Operation { __>RefLogEntry persist.proto
//         CREATE_REFERENCE = 0;
//         COMMIT = 1;
//         DELETE_REFERENCE = 2;
//         ASSIGN_REFERENCE = 3;
//         MERGE = 4;
//         TRANSPLANT = 5;
//         }*/
//
//        String op = refLog.getOperation();
//        AdapterTypes.RefLogEntry.Operation operation = AdapterTypes.RefLogEntry.Operation.TRANSPLANT;
//
//        /** Confirm whether the string ops are correct or not */
//        if(Objects.equals(op, "CREATE_REFERENCE"))
//        {
//            operation = AdapterTypes.RefLogEntry.Operation.CREATE_REFERENCE;
//        } else if (Objects.equals(op, "COMMIT")) {
//            operation = AdapterTypes.RefLogEntry.Operation.COMMIT;
//        } else if ( Objects.equals(op, "DELETE_REFERENCE") ) {
//            operation = AdapterTypes.RefLogEntry.Operation.COMMIT;
//        } else if (Objects.equals(op, "ASSIGN_REFERENCE") ) {
//            operation = AdapterTypes.RefLogEntry.Operation.ASSIGN_REFERENCE;
//        } else if (Objects.equals(op, "MERGE")) {
//            operation = AdapterTypes.RefLogEntry.Operation.MERGE;
//        }
//
//        AdapterTypes.RefLogEntry.Builder proto =
//                AdapterTypes.RefLogEntry.newBuilder()
//                        .setRefLogId(refLog.getRefLogId().asBytes())
//                        .setRefName(ByteString.copyFromUtf8(refLog.getRefName()))
//                        .setRefType(refType)
//                        .setCommitHash(refLog.getCommitHash().asBytes())
//                        .setOperationTime(refLog.getOperationTime())
//                        .setOperation(operation);
//
//        List<Hash> sourceHashes = refLog.getSourceHashes();
//        sourceHashes.forEach(hash -> proto.addSourceHashes(hash.asBytes()));
//
//        Stream<ByteString> parents = refLog.getParents().stream().map(Hash::asBytes);
//        parents.forEach(proto::addParents);
//
//        return proto.build();
//    }
//}
