import com.google.protobuf.ByteString;
import org.junit.Test;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.tx.*;
import org.projectnessie.versioned.persist.tx.postgres.PostgresDatabaseAdapterFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class TestExportPostgres {

    private String userName = "postgres" ;
    private String password = "postgrespw" ;

    private String serverName = "localhost";

    private String dbms = "postgresql";

    private String portNumber = "55002";

    @Test
    public void getTablesInPostgresRepo()
    {
        DatabaseAdapter postgresDBAdapter;

        StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();

        TxDatabaseAdapterConfig dbAdapterConfig = ImmutableAdjustableTxDatabaseAdapterConfig.builder().build();

        TxConnectionConfig txConnectionConfig = ImmutableDefaultTxConnectionConfig.builder().build();

        //Should initialize connector

        TxConnectionProvider<TxConnectionConfig> connector;

        connector = new TxConnectionProvider<TxConnectionConfig>() {
            @Override
            public Connection borrowConnection() throws SQLException {
                Connection conn;
                conn = DriverManager.getConnection("jdbc:postgresql://localhost:55002/nessie", "postgres", "postgrespw");
                conn.setAutoCommit(false);
                return conn;
            }

            @Override
            public void close() throws Exception {

            }
        };
        connector.configure(txConnectionConfig);

        try {
            connector.initialize();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        postgresDBAdapter = new PostgresDatabaseAdapterFactory()
                .newBuilder()
                .withConfig(dbAdapterConfig)
                .withConnector(connector)
                .build(storeWorker);

        try {
            Stream<ReferenceInfo<ByteString>> refs = postgresDBAdapter.namedRefs(GetNamedRefsParams.DEFAULT);
            refs.map(y -> y.getNamedRef().getName()).forEach(System.out::println);
        } catch (ReferenceNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
