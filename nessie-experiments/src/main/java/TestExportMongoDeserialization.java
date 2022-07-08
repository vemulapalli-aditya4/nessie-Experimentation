import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToRefLog;

public class TestExportMongoDeserialization {

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

        List<RefLog> originalRefLogTable = new ArrayList<>();
        FileOutputStream fosRefLog = null;
        try{

            fosRefLog = new FileOutputStream(refLogTableFilePath);
            FileOutputStream finalFosRefLog = fosRefLog;
            refLogTable.map(x-> {
                AdapterTypes.RefLogEntry refLogEntry = toProtoFromRefLog(x);
                originalRefLogTable.add(x);
                return refLogEntry.toByteArray();
            }).forEachOrdered(y ->{

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

            System.out.println("Ref Log Table is written");

        } catch(IOException e ) {
            throw new RuntimeException(e);
        }

        Path path = Paths.get("/Users/aditya.vemulapalli/Downloads/refLogTable");
        try {
            byte[] data = Files.readAllBytes(path);
            int noOfBytes = data.length;
            List<RefLog> deserializedRefLogTable = new ArrayList<RefLog>();
            // ByteBuffer byteBuffer = ByteBuffer.wrap(data);
            int from = 0 ;
            int size;
            byte[] sizeArr;
            byte[] obj;
            while(noOfBytes != 0)
            {
                sizeArr = Arrays.copyOfRange(data, from, from + 4);
                size = new BigInteger(sizeArr).intValue();
                from += 4;
                noOfBytes -= 4;
                obj = Arrays.copyOfRange(data, from , from + size );
                from += size;
                noOfBytes -= size;
                deserializedRefLogTable.add(protoToRefLog(obj));
            }

            System.out.println(originalRefLogTable.size());
            System.out.println("Number of deser.. RefLog entries are " + deserializedRefLogTable.size());
//            for(int i = 0 ; i <originalRefLogTable.size(); i++ )
//            {
//                System.out.println("Deser. " + originalRefLogTable.get(i).getCommitHash());
//                System.out.println("Org... " + deserializedRefLogTable.get(i).getCommitHash());
//                System.out.println("Org... " + originalRefLogTable.get(i).getRefLogId());
//                System.out.println("Deser. " + deserializedRefLogTable.get(i).getRefLogId());
//                System.out.println("Org... " + originalRefLogTable.get(i).getRefName());
//                System.out.println("Deser. " + deserializedRefLogTable.get(i).getRefName());
//                System.out.println("Org... " + originalRefLogTable.get(i).getRefType());
//                System.out.println("Deser. " + deserializedRefLogTable.get(i).getRefType());
//                System.out.println("Org... " + originalRefLogTable.get(i).getOperation());
//                System.out.println("Deser. " + deserializedRefLogTable.get(i).getOperation());
//                System.out.println("Org... " + originalRefLogTable.get(i).getOperationTime());
//                System.out.println("Deser. " + deserializedRefLogTable.get(i).getOperationTime());
//                System.out.println("Org... " + originalRefLogTable.get(i).getSourceHashes());
//                System.out.println("Deser. " + deserializedRefLogTable.get(i).getSourceHashes());
//                System.out.println("Org... " + originalRefLogTable.get(i).getParents());
//                System.out.println("Deser. " + deserializedRefLogTable.get(i).getParents());

//            }
            // byte[] len = new byte[4];
            System.out.println("lCalc is " + lCalc[0]);
            System.out.println("data arr length is " + data.length);
            //use this byte array to reconstruct the ref Log Table
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

        String commitLogTableFilePath = "/Users/aditya.vemulapalli/Downloads/commitLogFileTemp";

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

        CommitLogClassWrapped object = new CommitLogClassWrapped(new ArrayList<CommitLogClass>());

        FileOutputStream fileOut = null;
        try{
            fileOut = new FileOutputStream(commitLogTableFilePath);

            commitLogTable.map(x -> {
                long createdTime = x.getCreatedTime();

                String hash = x.getHash().asString();

                long commitSeq = x.getCommitSeq();

                String parent_1st = x.getParents().get(0).asString();

                ByteString metaDataByteString = x.getMetadata();
                CommitMeta commitMeta = metaSerializer.fromBytes(metaDataByteString);

                //puts
                List<KeyWithBytes> puts = x.getPuts();

                List<Content> contents = new ArrayList<>();
                List<ContentId> contentIds = new ArrayList<ContentId>();
                List<Key> putsKeys = new ArrayList<Key>();
                List<Key> deletes = new ArrayList<Key>();
                deletes = x.getDeletes();

                List<Hash> hashAdditionalParents = x.getAdditionalParents();
                List<String> additionalParents = new ArrayList<String>();
                for (Hash hashAdditionalParent : hashAdditionalParents) {
                    additionalParents.add(hashAdditionalParent.asString());
                }

                for (KeyWithBytes put : puts) {

                    contentIds.add(put.getContentId());

                    ByteString value = put.getValue();
                    Content content = storeWorker.valueFromStore(value, () -> getGlobalContents.apply(put));
                    contents.add(content);

                    Key key = put.getKey();
                    putsKeys.add(put.getKey());
                }

                return new CommitLogClass(createdTime, hash, commitSeq, parent_1st, commitMeta, contentIds, contents,
                        putsKeys, deletes, additionalParents);
            }).forEachOrdered(y -> {object.commitLog.add(y) ; });

            ObjectMapper objectMapper = new ObjectMapper();
            byte[] array;

            try{
                array = objectMapper.writeValueAsBytes(object);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            try{
                fileOut.write(array);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            fileOut.close();
            commitLogTable.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Path path = Paths.get("/Users/aditya.vemulapalli/Downloads/commitLogFileTemp" );

        try{
            byte[] data = Files.readAllBytes(path);
            ObjectMapper objectMapper1 = new ObjectMapper();
            CommitLogClassWrapped commitLogClassWrapped;
            commitLogClassWrapped = objectMapper1.readValue(data, CommitLogClassWrapped.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


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
            operation = AdapterTypes.RefLogEntry.Operation.DELETE_REFERENCE;
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
