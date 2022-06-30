//import org.projectnessie.client.api.NessieApiV1;
//import org.projectnessie.client.http.HttpClientBuilder;
//import org.projectnessie.client.rest.NessieInternalServerException;
//import org.projectnessie.client.rest.NessieNotAuthorizedException;
//import org.projectnessie.error.NessieNotFoundException;
//import org.projectnessie.model.LogResponse;
//
//public class TestNessie {
//    public static void main(String[] args) throws NessieNotFoundException {
//
//        NessieApiV1 api =
//                HttpClientBuilder.builder()
//                        .withUri("http://localhost:19120/api/v1")
//                        .build(NessieApiV1.class);
//
//        LogResponse commitLog = api.getCommitLog().refName("main").get();
//        //System.out.println(commitLog.getLogEntries());
//        for(LogResponse.LogEntry logEntry : commitLog.getLogEntries())
//        {
//            System.out.println("Metadata : " + logEntry.getCommitMeta());
//            System.out.println("Operations: " + logEntry.getOperations());
//            System.out.println("Parent Hash: " + logEntry.getParentCommitHash());
//        }
//
//        //getAllRefs
//
//        //getAllContents
//
//        //getRefLog
//    }
//
//}
