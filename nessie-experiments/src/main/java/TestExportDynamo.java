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
import org.projectnessie.versioned.persist.dynamodb.*;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.stream.Stream;

public class TestExportDynamo {

    @Test
    public void getTablesInDynamoRepo()
    {
        //Must initialize AWSCredentialsProvider and DynamoDbClient
        String  endpointURI = "http://localhost:8000";
        String region = "us-west-2";
        AwsCredentialsProvider credentialsProvider ;
        DynamoDbClient dynamoDbClient ;

        DynamoClientConfig dynamoClientConfig = ImmutableDefaultDynamoClientConfig
                .builder()
                .endpointURI(endpointURI)
                .region(region)
//                .credentialsProvider(credentialsProvider)
//                .dynamoDbClient(dynamoDbClient)
                .build();

        StoreWorker<Content, CommitMeta, Content.Type> storeWorker = new TableCommitMetaStoreWorker();

        NonTransactionalDatabaseAdapterConfig dynamoDbAdapterConfig = ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder().build();

        DatabaseAdapter dynamoDatabaseAdapter;

        try (DynamoDatabaseClient dynamoDatabaseClient = new DynamoDatabaseClient()) {

            dynamoDatabaseClient.configure(dynamoClientConfig);
            dynamoDatabaseClient.initialize();

            dynamoDatabaseAdapter = new DynamoDatabaseAdapterFactory()
                    .newBuilder()
                    .withConnector(dynamoDatabaseClient)
                    .withConfig( dynamoDbAdapterConfig )
                    .build(storeWorker);

        }

        try {
            Stream<ReferenceInfo<ByteString>> refs = dynamoDatabaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT);
        } catch (ReferenceNotFoundException e) {
            throw new RuntimeException(e);
        }


    }
}
