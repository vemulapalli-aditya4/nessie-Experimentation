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

    private String portNumber = "55001";

    public final AtomicReference<Connection> postgresConnection = new AtomicReference<>();

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

                try{
                    if(postgresConnection.get() == null && postgresConnection.get().isClosed())
                    {
                        postgresConnection.compareAndSet(null, getConnection());
                    }

                    postgresConnection.get().setAutoCommit(false);
                    return  postgresConnection.get();
                }
                catch(SQLException e)
                {
                    System.out.println(" SQL Exception and NULL connection is returned " + e.getMessage());

                    return null;
                }

            }

            @Override
            public void close() throws Exception {

            }
        };

        connector.configure(txConnectionConfig);

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


        try {
            postgresConnection.get().close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public Connection getConnection() throws SQLException{
        Connection conn = null;

        String jdbcURL = "jdbc:postgresql://localhost:55000/nessie";
        conn = DriverManager.getConnection(jdbcURL, userName, password);

        return conn;
    }

}
