package pt.isec.msh;

import java.sql.SQLException;

/**
 * Created by Rui on 24/02/2016.
 */
public  class MSConfig {

    private static MSClient mysql;

    public static void Init() {
        try {

//CRIAR BD
            mysql = new MSClient();

            StringBuilder _sb;
            if (mysql.TableExists(mysql.table_prefix + "_CONFIG")) {
                _sb = new StringBuilder();
                _sb.append("");
                _sb.append("");
                _sb.append("");
                _sb.append("");
                _sb.append("");
                _sb.append("");
                _sb.append("");
                _sb.append("");
                _sb.append("");

                mysql.ExecuteQuery(_sb.toString());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static String GetString(String Key) {
        return null;
    }

    public static void WriteString(String Key, String Value) {

    }

}
