package pt.isec.msh;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

/**
 * Created by Rui on 20/02/2016.
 */
public class MS_StatementHlp {

    public static void SetString(PreparedStatement stmt, int Index, Object value) throws SQLException {
        if (value == null) {
            stmt.setNull(Index, Types.VARCHAR);
        } else {
            stmt.setString(Index, value.toString());
        }
    }

    public static void SetDecimal(PreparedStatement stmt, int Index, Object value) throws SQLException {
        if (value == null) {
            stmt.setNull(Index, Types.DECIMAL);
        } else {
            stmt.setDouble(Index, new Double(value.toString()));
        }
    }


    public static void SetDecimal2(PreparedStatement stmt, int Index, double value) throws SQLException {
        stmt.setDouble(Index, value);
    }

    public static void SetInt(PreparedStatement stmt, int Index, Object value) throws SQLException {
        if (value == null) {
            stmt.setInt(Index, Types.DECIMAL);
        } else {
            stmt.setInt(Index, new Integer(value.toString()));
        }
    }

}
