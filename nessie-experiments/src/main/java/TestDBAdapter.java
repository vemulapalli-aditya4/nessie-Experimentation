//import com.google.protobuf.ByteString;
//import com.mongodb.client.MongoCollection;
//import org.bson.Document;
//import org.jetbrains.annotations.TestOnly;
//import org.junit.Test;
//import java.util.*;
//import org.projectnessie.model.CommitMeta;
//import org.projectnessie.model.Content;
//import org.projectnessie.quarkus.providers.TransactionalConnectionProvider;
//import org.projectnessie.server.store.TableCommitMetaStoreWorker;
//import org.projectnessie.versioned.*;
//import org.projectnessie.versioned.persist.adapter.*;
//import org.projectnessie.versioned.persist.mongodb.*;
//import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;
//import org.projectnessie.versioned.persist.nontx.NonTransactionalOperationContext;
//import org.projectnessie.versioned.persist.serialize.AdapterTypes;
//import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapter;
//
//import java.util.Optional;
//import java.util.Spliterator;
//import java.util.stream.Stream;
//
//public class TestDBAdapter {
//
//    @Test
//    public void TestGetRefs() throws ReferenceNotFoundException {
//        MongoClientConfig mongoClientConfig = ImmutableMongoClientConfig.builder()
//        .connectionString("mongodb://root:password@localhost:27017").databaseName("nessie").build();
//
//        MongoDatabaseClient MongoDBClient = new MongoDatabaseClient();
//        MongoDBClient.configure(mongoClientConfig);
//        MongoDBClient.initialize();
//        System.out.println("Initialized");
//
//        System.out.println("Count of reflog is " + MongoDBClient.getRefLog().countDocuments());
//        System.out.println("Count of commitlog is  " + MongoDBClient.getCommitLog().countDocuments());
//
//        StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();
//
//        DatabaseAdapter mongoDatabaseAdapter = new MongoDatabaseAdapterFactory()
//                .newBuilder()
//                .withConnector(MongoDBClient)
//                .withConfig(ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder().build())
//                .build(storeWorker);
//
//        System.out.println("DatabaseAdapter Initialized");
//
//        System.out.println("Branch names");
//        Stream<ReferenceInfo<ByteString>> refs = mongoDatabaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT);
//
//        refs.map(r -> r.getNamedRef().getName()).forEach(System.out::println);
//    }
//
//    @Test
//    public void TablesInMongoRepo() throws ReferenceNotFoundException {
//
//        MongoClientConfig mongoClientConfig = ImmutableMongoClientConfig.builder()
//                .connectionString("mongodb://root:password@localhost:27017").databaseName("nessie").build();
//
//        MongoDatabaseClient mongoDBClient = new MongoDatabaseClient();
//        mongoDBClient.configure(mongoClientConfig);
//        mongoDBClient.initialize();
//
//        System.out.println("Mongo Database Client Initialized");
//
//        MongoCollection<Document> commitLogTable = mongoDBClient.getCommitLog();
//        MongoCollection<Document> refLogTable = mongoDBClient.getRefLog();
//        MongoCollection<Document> repoDescTable = mongoDBClient.getRepoDesc();
//        MongoCollection<Document> globalPointersTable= mongoDBClient.getGlobalPointers();
//        MongoCollection<Document> globalLogTable = mongoDBClient.getGlobalLog();
//        MongoCollection<Document> keyListsTable = mongoDBClient.getKeyLists() ;
//
//        StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();
//
//        MongoDatabaseAdapter mongoDatabaseAdapter = (MongoDatabaseAdapter) new MongoDatabaseAdapterFactory()
//                .newBuilder()
//                .withConnector(mongoDBClient)
//                .withConfig(ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder().build())
//                .build(storeWorker);
//
//        System.out.println("Mongo DatabaseAdapter Initialized");
//
//
//        NonTransactionalOperationContext ctx = NonTransactionalOperationContext.NON_TRANSACTIONAL_OPERATION_CONTEXT;
//
//        //commit log Table
//        Stream<CommitLogEntry> allCommitLogEntries =  mongoDatabaseAdapter.scanAllCommitLogEntries();
//
//        //ref log table
//        Hash initialHash;
//        // Stream <RefLog> allRefLogEntries = mongoDatabaseAdapter.refLog(initialHash);
//
//
//        // Repo Desc Table
//        RepoDescription repoDesc = mongoDatabaseAdapter.fetchRepositoryDescription();
//
//        System.out.println(" " + repoDesc.getRepoVersion());
//
//        //Key Lists Table
//        List<Hash> keyListsIds;
//
//        // Stream<KeyListEntity> keysListsTable = mongoDatabaseAdapter.fetchKeyLists(ctx,keyListsIds );
//
//        //Global-state-log Table
//        ///** Reads from the global-state-log starting at the given global-state-log-ID. */
//        // private Stream<AdapterTypes.GlobalStateLogEntry> globalLogFetcher(NonTransactionalOperationContext ctx) {
//        // Stream<AdapterTypes.GlobalStateLogEntry> globalStateLogTable = mongoDatabaseAdapter.globalLogFetcher( ctx);
//
//        // Global - pointer Table
//        // AdapterTypes.GlobalStatePointer gsp = mongoDatabaseAdapter.fetchGlobalPointer(ctx);
//
//        // ref log head Table
//
//
//        //Ref names Table
//
//
//        //ref heads table
//
//        Stream<ReferenceInfo<ByteString>> refs = mongoDatabaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT);
//
//        // protected Spliterator<RefLog> readRefLog(NonTransactionalOperationContext ctx, Hash initialHash)
//
//
//
//
//
//
//
//    }
//
//
//}
