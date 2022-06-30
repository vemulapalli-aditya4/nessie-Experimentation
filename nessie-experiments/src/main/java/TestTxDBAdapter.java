//import com.google.protobuf.ByteString;
//import org.junit.Test;
//import org.projectnessie.model.CommitMeta;
//import org.projectnessie.model.Content;
//import org.projectnessie.quarkus.providers.TransactionalConnectionProvider;
//import org.projectnessie.server.store.TableCommitMetaStoreWorker;
//import org.projectnessie.versioned.*;
//import org.projectnessie.versioned.persist.adapter.*;
//import org.projectnessie.versioned.persist.tx.*;
//import org.projectnessie.versioned.persist.tx.postgres.PostgresDatabaseAdapter;
//import org.projectnessie.versioned.persist.tx.postgres.PostgresDatabaseAdapterFactory;
//
//import java.sql.SQLException;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.stream.Stream;
//
//public class TestTxDBAdapter {
//
//    @Test
//    public  void  TestGetRefs() throws ReferenceNotFoundException, SQLException {
//
//        //Anything called PostgresSQLClientConfig
//
//        //anything which initializes postgres DB client
//
//
//        StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();
//
//        //with connector ??
//
//        TxDatabaseAdapterConfig dbAdapterConfig = ImmutableAdjustableTxDatabaseAdapterConfig.builder().build();
//        // DatabaseConnectionProvider<CONN_CONFIG> getConnectionProvider();
//
//        TxConnectionConfig txConnectionConfig = ImmutableDefaultTxConnectionConfig.builder().build();
//
//        //////TransactionalConnectionprovider
//
//        TransactionalConnectionProvider transactionalConnectionProvider = new TransactionalConnectionProvider();
//
//        TxConnectionProvider<TxConnectionConfig> connector;
//        connector = transactionalConnectionProvider.produceConnectionProvider();
//
//        connector.configure(txConnectionConfig);
//
//        PostgresDatabaseAdapter postgresDBAdapter = (PostgresDatabaseAdapter) new PostgresDatabaseAdapterFactory()
//                .newBuilder()
//                .withConfig(dbAdapterConfig)
//                .withConnector(connector)
//                .build(storeWorker);
//
//        System.out.println("Postgres DatabaseAdapter Initialized");
//
//        System.out.println("Branch names");
//
//        Stream<ReferenceInfo<ByteString>> refs = postgresDBAdapter.namedRefs(GetNamedRefsParams.DEFAULT);
//
//        refs.map(r -> r.getNamedRef().getName()).forEach(System.out::println);
//
//    }
//
//    @Test
//    public void TablesInPostgresRepo() throws ReferenceNotFoundException, SQLException {
//
//        StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();
//
//        TxDatabaseAdapterConfig dbAdapterConfig = ImmutableAdjustableTxDatabaseAdapterConfig.builder().build();
//
//        TxConnectionConfig txConnectionConfig = ImmutableDefaultTxConnectionConfig.builder().build();
//
//        TransactionalConnectionProvider transactionalConnectionProvider = new TransactionalConnectionProvider();
//
//        TxConnectionProvider<TxConnectionConfig> connector;
//
//        connector = transactionalConnectionProvider.produceConnectionProvider();
//
//        connector.configure(txConnectionConfig);
//
//        PostgresDatabaseAdapter postgresDBAdapter = (PostgresDatabaseAdapter) new PostgresDatabaseAdapterFactory()
//                .newBuilder()
//                .withConfig(dbAdapterConfig)
//                .withConnector(connector)
//                .build(storeWorker);
//
//        System.out.println("Postgres DatabaseAdapter Initialized");
//
//        ConnectionWrapper ctx = PostgresDatabaseAdapter.borrowConnection();
//
//        //Repo Desc Table
//        RepoDescription repoDescTable = postgresDBAdapter.fetchRepositoryDescription();
//
//        //Named References Table
//        Stream<ReferenceInfo<ByteString>> refs = postgresDBAdapter.namedRefs(GetNamedRefsParams.DEFAULT);
//
//        //Global State Table : Protected
//        Set<ContentId> contentIds;
//        Map<ContentId, ByteString> globalStateTable = postgresDBAdapter.fetchGlobalStates(ctx, contentIds);
//
//        // Commit Log table
//        Stream<CommitLogEntry> allCommitLogEntries =  PostgresDatabaseAdapter.scanAllCommitLogEntries();
//
//        //Key-lists Table
//
//        List<Hash> keyListsIds;
//        Stream<KeyListEntity> keysListsTable = PostgresDatabaseAdapter.fetchKeyLists(ctx,keyListsIds );
//
//        //Ref Log Head table
//        // RefLogHead refLogHeadTable = PostgresDatabaseAdapter.getRefLogHead(ctx);
//
//        //Ref Log Table
//        Hash initialHash;
//
//        Stream <RefLog> allRefLogEntries = postgresDBAdapter.refLog(initialHash);
//
//
//
//
//
//
//
//    }
//}
//
