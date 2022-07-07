import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonStreamParser;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.ByteString;
import org.junit.Test;
import org.projectnessie.model.*;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.*;
import org.projectnessie.versioned.persist.adapter.*;
import org.projectnessie.versioned.persist.mongodb.ImmutableMongoClientConfig;
import org.projectnessie.versioned.persist.mongodb.MongoClientConfig;
import org.projectnessie.versioned.persist.mongodb.MongoDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.mongodb.MongoDatabaseClient;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.serialize.AdapterTypes;

import javax.annotation.Nullable;
import java.io.*;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.*;



public class TestExportFunctionality {

    @Test
    public void TestRefLogTable() throws RefLogNotFoundException {
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

        Stream<RefLog> refLogTable = mongoDatabaseAdapter.refLog(null);

        String refLogTableFilePath = "/Users/aditya.vemulapalli/Downloads/refLogTable";

        final int[] lCalc = {0};

        FileOutputStream fosRefLog = null;
        try{

            fosRefLog = new FileOutputStream(refLogTableFilePath);
            FileOutputStream finalFosRefLog = fosRefLog;
            refLogTable.map(x-> {
                AdapterTypes.RefLogEntry refLogEntry = toProtoFromRefLog(x);
                return refLogEntry.toByteArray();
            }).forEach(y ->{

                int len = y.length;
                lCalc[0] += len;
                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(len);
                byte[] bytes = bb.array();
                lCalc[0] += bytes.length;

                try {
                    finalFosRefLog.write(bytes);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                try {
                    finalFosRefLog.write(y);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            fosRefLog.close();

            finalFosRefLog.close();
            refLogTable.close();

        } catch(IOException e ) {
            throw new RuntimeException(e);
        }

        Path path = Paths.get("/Users/aditya.vemulapalli/Downloads/refLogTable");
        try {
         byte[] data = Files.readAllBytes(path);
         System.out.println("lCalc is " + lCalc[0]);
         System.out.println("data arr length is " + data.length);
         //use this byte array to reconstruct the ref Log Table
        } catch (IOException e) {
         throw new RuntimeException(e);
         }

    }

    @Test
    public void TestNamedRefs() throws RefLogNotFoundException {
        MongoClientConfig mongoClientConfig = ImmutableMongoClientConfig.builder()
                .connectionString("mongodb://root:password@localhost:27017").databaseName("nessie").build();

        MongoDatabaseClient MongoDBClient = new MongoDatabaseClient();
        MongoDBClient.configure(mongoClientConfig);
        MongoDBClient.initialize();
        System.out.println("Mongo DB Client Initialized");

        System.out.println("Count of reflog is " + MongoDBClient.getRefLog().countDocuments());
        System.out.println("Count of commitlog is  " + MongoDBClient.getCommitLog().countDocuments());

        long count = MongoDBClient.getCommitLog().countDocuments();
        StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();

        DatabaseAdapter mongoDatabaseAdapter = new MongoDatabaseAdapterFactory()
                .newBuilder()
                .withConnector(MongoDBClient)
                .withConfig(ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder().build())
                .build(storeWorker);
        System.out.println("DatabaseAdapter Initialized");

        GetNamedRefsParams params = GetNamedRefsParams.DEFAULT;

        String namedRefsFilePath = "/Users/aditya.vemulapalli/Downloads/namedRefs";

        List<ReferenceInfoExport> namedRefsInfoList;
        namedRefsInfoList = new ArrayList<ReferenceInfoExport>();
        Stream<ReferenceInfo<ByteString>> namedReferences = null;
        FileOutputStream fileOut = null;
        ObjectOutputStream out = null;

        try {
            namedReferences = mongoDatabaseAdapter.namedRefs(params);
        } catch (ReferenceNotFoundException e) {
            throw new RuntimeException(e);
        }

        try {
            fileOut = new FileOutputStream(namedRefsFilePath);
            out = new ObjectOutputStream(fileOut);

            namedReferences.map(x -> {
                String referenceName = x.getNamedRef().getName();

                String type  = " "; /** must get this */
                if(x.getNamedRef() instanceof ImmutableBranchName)
                {
                    type = "branch";
                } else if (x.getNamedRef() instanceof ImmutableTagName) {
                    type = "tag";
                }

                String hash = x.getHash().asString();

                return  new ReferenceInfoExport(referenceName, type, hash);
            }).forEach(namedRefsInfoList::add);


            out.writeObject(namedRefsInfoList);
            out.close();
            fileOut.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        FileInputStream fileIn = null;
         ObjectInputStream in = null;
         List<ReferenceInfoExport> readNamedRefsInfoList = new ArrayList<ReferenceInfoExport>();
         try{
         fileIn = new FileInputStream(namedRefsFilePath);
         in = new ObjectInputStream(fileIn);

         readNamedRefsInfoList = (ArrayList) in.readObject();
         in.close();
         fileIn.close();

             for (ReferenceInfoExport referenceInfoExport : readNamedRefsInfoList) {
                 System.out.println("" + referenceInfoExport.referenceName);
                 System.out.println("" + referenceInfoExport.type);
                 System.out.println("" + referenceInfoExport.hash);
             }
         } catch (IOException | ClassNotFoundException e) {
         throw new RuntimeException(e);
         }
    }

    @Test
    public void TestRepoDesc()
    {
        MongoClientConfig mongoClientConfig = ImmutableMongoClientConfig.builder()
                .connectionString("mongodb://root:password@localhost:27017").databaseName("nessie").build();

        MongoDatabaseClient MongoDBClient = new MongoDatabaseClient();
        MongoDBClient.configure(mongoClientConfig);
        MongoDBClient.initialize();
        System.out.println("Mongo DB Client Initialized");

        System.out.println("Count of reflog is " + MongoDBClient.getRefLog().countDocuments());
        System.out.println("Count of commitlog is  " + MongoDBClient.getCommitLog().countDocuments());

        long count = MongoDBClient.getCommitLog().countDocuments();
        StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();

        DatabaseAdapter mongoDatabaseAdapter = new MongoDatabaseAdapterFactory()
                .newBuilder()
                .withConnector(MongoDBClient)
                .withConfig(ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder().build())
                .build(storeWorker);
        System.out.println("DatabaseAdapter Initialized");

        RepoDescription repoDescTable = mongoDatabaseAdapter.fetchRepositoryDescription();
        System.out.println("Repo version is " + repoDescTable.getRepoVersion());
        AdapterTypes.RepoProps repoProps = toProto(repoDescTable);

        /**String repoDescFilePath = targetDirectory + "/repoDesc"*/
        String repoDescFilePath = "/Users/aditya.vemulapalli/Downloads/repoDesc";

        byte[] arr = repoProps.toByteArray();
        FileOutputStream fosDescTable = null;
        try{
            fosDescTable = new FileOutputStream(repoDescFilePath);
            fosDescTable.write(arr);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if(fosDescTable != null)
            {
                try {
                    fosDescTable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        /** Deserialization Logic*/

        Path path = Paths.get("/Users/aditya.vemulapalli/Downloads/repoDesc");
         try {
         byte[] data = Files.readAllBytes(path);
         RepoDescription repoDesc = protoToRepoDescription(data);
             System.out.println("Repo version is " + repoDesc.getRepoVersion());
         } catch (IOException e) {
         throw new RuntimeException(e);
         }

    }

    @Test
    public void TestCommitLogTable()
    {
        MongoClientConfig mongoClientConfig = ImmutableMongoClientConfig.builder()
                .connectionString("mongodb://root:password@localhost:27017").databaseName("nessie").build();

        MongoDatabaseClient MongoDBClient = new MongoDatabaseClient();
        MongoDBClient.configure(mongoClientConfig);
        MongoDBClient.initialize();
        System.out.println("Mongo DB Client Initialized");

        System.out.println("Count of reflog is " + MongoDBClient.getRefLog().countDocuments());
        System.out.println("Count of commitlog is  " + MongoDBClient.getCommitLog().countDocuments());

        long count = MongoDBClient.getCommitLog().countDocuments();
        StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();

        DatabaseAdapter mongoDatabaseAdapter = new MongoDatabaseAdapterFactory()
                .newBuilder()
                .withConnector(MongoDBClient)
                .withConfig(ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder().build())
                .build(storeWorker);
        System.out.println("DatabaseAdapter Initialized");

        String commitLogTableFilePath1 = "/Users/aditya.vemulapalli/Downloads/commitLogFile1";
        String commitLogTableFilePath2 = "/Users/aditya.vemulapalli/Downloads/commitLogFile2";
        String commitLogTableFilePath3 = "/Users/aditya.vemulapalli/Downloads/commitLogFile3";

        Stream<CommitLogEntry> commitLogTable =  mongoDatabaseAdapter.scanAllCommitLogEntries();

        /**entries bounded cache*/
        Map<ContentId, ByteString> globalContents = new HashMap<>();
        Function<KeyWithBytes, ByteString> getGlobalContents =
                (put) ->
                        globalContents.computeIfAbsent(
                                put.getContentId(),
                                cid ->
                                        mongoDatabaseAdapter
                                                .globalContent(put.getContentId())
                                                .map(ContentIdAndBytes::getValue)
                                                .orElse(null));

        Serializer<CommitMeta> metaSerializer = storeWorker.getMetadataSerializer();

        List<CommitLogClass1> commitLogList1 = new ArrayList<CommitLogClass1>();
        List<CommitLogClass2> commitLogList2 = new ArrayList<CommitLogClass2>();
        List<CommitLogClass3> commitLogList3 = new ArrayList<CommitLogClass3>();

        FileOutputStream fileOut1 = null;
        ObjectOutputStream out1 = null;
        FileOutputStream fileOut2 = null;
        FileOutputStream fileOut3 = null;
        try{
            fileOut1 = new FileOutputStream(commitLogTableFilePath1);
            out1 = new ObjectOutputStream(fileOut1);
            fileOut2 = new FileOutputStream(commitLogTableFilePath2);
            fileOut3 = new FileOutputStream(commitLogTableFilePath3);

            commitLogTable.map(x -> {
                long createdTime = x.getCreatedTime();
                long commitSeq = x.getCommitSeq();
                String hash = x.getHash().asString();

                String parent_1st = x.getParents().get(0).asString();

                List<String> additionalParents = new ArrayList<String>();

                List<Hash> hashAdditionalParents = x.getAdditionalParents();
                for (Hash hashAdditionalParent : hashAdditionalParents) {
                    additionalParents.add(hashAdditionalParent.asString());
                }

                List<String> deletes = new ArrayList<String>();
                List<Integer> noOfStringsInKeys = new ArrayList<Integer>();

                List<Key> keyDeletes = x.getDeletes();
                for (Key keyDelete : keyDeletes) {

                    List<String> elements = keyDelete.getElements();

                    noOfStringsInKeys.add(elements.size());

                    deletes.addAll(elements);
                }

                List<KeyWithBytes> puts = x.getPuts();

                ByteString metaDataByteString = x.getMetadata();

                CommitMeta metaData = metaSerializer.fromBytes(metaDataByteString);

                commitLogList3.add(new CommitLogClass3(metaData));

                List <String> contentIds = new ArrayList<>();
                List<Content> contents = new ArrayList<>();
                List<String> putsKeyStrings = new ArrayList<>();
                List<Integer> putsKeyNoOfStrings = new ArrayList<>();

                for (KeyWithBytes put : puts) {
                    ContentId contentId = put.getContentId();
                    contentIds.add(contentId.getId());

                    ByteString value = put.getValue();

                    Content content = storeWorker.valueFromStore(value, () -> getGlobalContents.apply(put));

                    contents.add(content);

                    Key key = put.getKey();
                    List<String> elements1 = key.getElements();
                    putsKeyNoOfStrings.add(elements1.size());
                    putsKeyStrings.addAll(elements1);
                }

                commitLogList2.add(new CommitLogClass2(contents));

                /** Must Change This */
                return new CommitLogClass1(createdTime, commitSeq, hash, parent_1st, additionalParents, deletes, noOfStringsInKeys,
                        contentIds, putsKeyStrings, putsKeyNoOfStrings);
            }).forEach(commitLogList1::add);

            for (CommitLogClass2 commitLogClass2 : commitLogList2) {
                //First store the number of contents in each commit log entry
                ByteBuffer bb1 = ByteBuffer.allocate(4);
                int noOfContents = commitLogClass2.contents.size();
                bb1.putInt(noOfContents);
                byte[] bytes1 = bb1.array();
                try {
                    fileOut2.write(bytes1);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                for (int j = 0; j < noOfContents; j++) {
                    ObjectMapper objectMapper = new ObjectMapper();
                    byte[] arr;
                    try {
                        arr = objectMapper.writeValueAsBytes(commitLogClass2.contents.get(j));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    ByteBuffer bb2 = ByteBuffer.allocate(4);
                    bb2.putInt(arr.length);
                    byte[] bytes2 = bb2.array();
                    try {
                        fileOut2.write(bytes2);
                        fileOut2.write(arr);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                }

            }

            for(CommitLogClass3 commitLogClass3 : commitLogList3)
            {
                ObjectMapper objectMapper = new ObjectMapper();
                byte[] array;

                try{
                    array = objectMapper.writeValueAsBytes(commitLogClass3.metaData);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }

                ByteBuffer bb = ByteBuffer.allocate(4);
                bb.putInt(array.length);
                byte[] lenOfCommitMeta = bb.array();

                try{
                    fileOut3.write(lenOfCommitMeta);
                    fileOut3.write(array);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            out1.writeObject(commitLogList1);
            out1.close();
            fileOut1.close();
            fileOut2.close();
            fileOut3.close();
            commitLogTable.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        /** Deserialization Logic for CommitLogFile1*/
        /**
         * FileInputStream fileIn = null;
         *         ObjectInputStream in = null;
         *         List<CommitLogClass1> commitLogClass1List = new ArrayList<CommitLogClass1>();
         *         try{
         *          fileIn = new FileInputStream(commitLogTableFilePath1);
         *          in = new ObjectInputStream(fileIn);
         *          commitLogClass1List = (ArrayList) in.readObject();
         *          in.close();
         *          fileIn.close();
         *
         *          for(CommitLogClass1 commitLogClass1 : commitLogClass1List)
         *          {
         *              System.out.println(commitLogClass1.commitSeq);
         *              System.out.println(commitLogClass1.createdTime);
         *              System.out.println(commitLogClass1.parent_1st);
         *              System.out.println(commitLogClass1.hash);
         *              System.out.println(commitLogClass1.additionalParents);
         *              System.out.println(commitLogClass1.contentIds);
         *              System.out.println(commitLogClass1.deletes);
         *              System.out.println(commitLogClass1.noOfStringsInKeys);
         *              System.out.println(commitLogClass1.putsKeyStrings);
         *              System.out.println(commitLogClass1.putsKeyNoOfStrings);
         *          }
         *
         *          } catch (IOException | ClassNotFoundException e) {
         *          throw new RuntimeException(e);
         *          }*/

    }
        public AdapterTypes.RefLogEntry toProtoFromRefLog(RefLog refLog)
    {
        /** Reference type can be 'Branch' or 'Tag'. */
        AdapterTypes.RefType refType = Objects.equals(refLog.getRefType(), "Tag") ? AdapterTypes.RefType.Tag : AdapterTypes.RefType.Branch;
        /**enum Operation { __>RefLogEntry persist.proto
         CREATE_REFERENCE = 0;
         COMMIT = 1;
         DELETE_REFERENCE = 2;
         ASSIGN_REFERENCE = 3;
         MERGE = 4;
         TRANSPLANT = 5;
         }*/

        String op = refLog.getOperation();
        AdapterTypes.RefLogEntry.Operation operation = AdapterTypes.RefLogEntry.Operation.TRANSPLANT;

        /** Confirm whether the string ops are correct or not */
        if(Objects.equals(op, "CREATE_REFERENCE"))
        {
            operation = AdapterTypes.RefLogEntry.Operation.CREATE_REFERENCE;
        } else if (Objects.equals(op, "COMMIT")) {
            operation = AdapterTypes.RefLogEntry.Operation.COMMIT;
        } else if ( Objects.equals(op, "DELETE_REFERENCE") ) {
            operation = AdapterTypes.RefLogEntry.Operation.COMMIT;
        } else if (Objects.equals(op, "ASSIGN_REFERENCE") ) {
            operation = AdapterTypes.RefLogEntry.Operation.ASSIGN_REFERENCE;
        } else if (Objects.equals(op, "MERGE")) {
            operation = AdapterTypes.RefLogEntry.Operation.MERGE;
        }

        AdapterTypes.RefLogEntry.Builder proto =
                AdapterTypes.RefLogEntry.newBuilder()
                        .setRefLogId(refLog.getRefLogId().asBytes())
                        .setRefName(ByteString.copyFromUtf8(refLog.getRefName()))
                        .setRefType(refType)
                        .setCommitHash(refLog.getCommitHash().asBytes())
                        .setOperationTime(refLog.getOperationTime())
                        .setOperation(operation);

        List<Hash> sourceHashes = refLog.getSourceHashes();
        sourceHashes.forEach(hash -> proto.addSourceHashes(hash.asBytes()));

        Stream<ByteString> parents = refLog.getParents().stream().map(Hash::asBytes);
        parents.forEach(proto::addParents);

        return proto.build();
    }
}
