//import java.sql.*;
//
//public class JDBCpostgres {
//
//    // jdbc:postgresql://<database_host>:<port>/<database_name>
//    // jdbc:postgresql://localhost:5432/my_database
//    private final String url = "jdbc:postgresql://localhost/nessie";
//    private final String user = "postgres";
//    private final String password = "password";
//
//
//    public Connection connect() {
//        Connection conn = null;
//        try {
//            conn = DriverManager.getConnection(url, user, password);
//            System.out.println("Connected to the PostgreSQL server successfully.");
//        } catch (SQLException e) {
//            System.out.println(e.getMessage());
//        }
//
//        return conn;
//    }
//
//    public ResultSet getRepoDescTable( ){
//        String SQL = "SELECT * FROM repo_desc";
//        //
//        ResultSet res = null;
//        try (Connection conn = connect();
//             Statement stmt = conn.createStatement();
//             ResultSet rs = stmt.executeQuery(SQL)) {
//
//            res = rs;
//        } catch (SQLException ex) {
//            System.out.println(ex.getMessage());
//        }
//
//        return res;
//    }
//
//    public ResultSet getNamedRefsTable( ){
//        String SQL = "SELECT * FROM named_refs";
//        //
//        ResultSet res = null;
//        try (Connection conn = connect();
//             Statement stmt = conn.createStatement();
//             ResultSet rs = stmt.executeQuery(SQL)) {
//
//            res = rs;
//        } catch (SQLException ex) {
//            System.out.println(ex.getMessage());
//        }
//
//        return res;
//    }
//
//    public ResultSet getGlobalStateTable( ){
//        String SQL = "SELECT * FROM global_state";
//        //
//        ResultSet res = null;
//        try (Connection conn = connect();
//             Statement stmt = conn.createStatement();
//             ResultSet rs = stmt.executeQuery(SQL)) {
//
//            res = rs;
//        } catch (SQLException ex) {
//            System.out.println(ex.getMessage());
//        }
//
//        return res;
//    }
//
//    public ResultSet getCommitLogTable( ){
//        String SQL = "SELECT * FROM commit_log";
//        //
//        ResultSet res = null;
//        try (Connection conn = connect();
//             Statement stmt = conn.createStatement();
//             ResultSet rs = stmt.executeQuery(SQL)) {
//
//            res = rs;
//        } catch (SQLException ex) {
//            System.out.println(ex.getMessage());
//        }
//
//        return res;
//    }
//
//    public ResultSet getKeyListTable( ){
//        String SQL = "SELECT * FROM key_list";
//        //
//        ResultSet res = null;
//        try (Connection conn = connect();
//             Statement stmt = conn.createStatement();
//             ResultSet rs = stmt.executeQuery(SQL)) {
//
//            res = rs;
//        } catch (SQLException ex) {
//            System.out.println(ex.getMessage());
//        }
//
//        return res;
//    }
//
//    public ResultSet getRefLogTable( ){
//        String SQL = "SELECT * FROM ref_log";
//        //
//        ResultSet res = null;
//        try (Connection conn = connect();
//             Statement stmt = conn.createStatement();
//             ResultSet rs = stmt.executeQuery(SQL)) {
//
//            res = rs;
//        } catch (SQLException ex) {
//            System.out.println(ex.getMessage());
//        }
//
//        return res;
//    }
//
//    public ResultSet getRefLogHeadTable( ){
//        String SQL = "SELECT * FROM ref_log_head";
//        //
//        ResultSet res = null;
//        try (Connection conn = connect();
//             Statement stmt = conn.createStatement();
//             ResultSet rs = stmt.executeQuery(SQL)) {
//
//            res = rs;
//        } catch (SQLException ex) {
//            System.out.println(ex.getMessage());
//        }
//
//        return res;
//    }
//
//    public static void main(String[] args) {
//
//        //
//    }
//}
