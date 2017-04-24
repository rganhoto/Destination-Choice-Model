package pt.isec.msh;

import java.sql.*;

/**
 * Created by Rui on 19/02/2016.
 */
public class MSClient {

    public static String mysql_address = "";
    public static int mysql_port = 0;
    public static String mysql_user = "";
    public static String mysql_password = "";
    public static String mysql_database = "";

    public static String table_prefix="";

    public Connection conn = null;

    public MSClient() {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void ExecuteQuery(String Query) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(Query);
    }

    public ResultSet getData(String Query) throws SQLException {
        ResultSet _rs = null;
        Statement stmt = conn.createStatement();
        if (stmt.execute(Query)) {
            _rs = stmt.getResultSet();
        }
        return _rs;
    }

    public String getString(String Query) throws SQLException {
        String _value=null;
        ResultSet _rs = null;
        Statement stmt = conn.createStatement();
        if (stmt.execute(Query)) {
            _rs = stmt.getResultSet();
            if(_rs.next())
                _value = _rs.getString(1);
        }
        _rs.close();
        stmt.close();
        return _value;
    }


    public void OpenConnection() throws SQLException {
        CloseConnectionIfOpen();
        conn = DriverManager.getConnection("jdbc:mysql://" + mysql_address + ":" + mysql_port + "/" + mysql_database + "?user=" + mysql_user + "&password=" + mysql_password +"&useSSL=false" );
        ExecuteQuery("SET NAMES utf8");
    }

    public void BeginTransaction() throws SQLException {
        conn.setAutoCommit(false);
    }

    public void CommitTransaction() throws SQLException {
        conn.commit();
        conn.setAutoCommit(true);
    }

    public void RollbackTransaction() throws SQLException {
        conn.rollback();
        conn.setAutoCommit(true);
    }

    public void CloseConnectionIfOpen() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        conn = null;
    }

    public PreparedStatement PrepareStatement(String Query) throws SQLException {
        BeginTransaction();
        PreparedStatement stmt = conn.prepareStatement(Query);
        return stmt;
    }

    public void FinishStatement() throws SQLException {
        CommitTransaction();
    }

    public boolean TableExists(String TableName) {
        String Query = "SHOW TABLES LIKE '" + TableName + "' ";
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(Query);
            return rs.next();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (rs != null) rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return false;
    }
}
