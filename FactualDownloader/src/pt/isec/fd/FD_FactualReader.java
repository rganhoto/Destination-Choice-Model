package pt.isec.fd;

import com.factual.driver.Factual;
import com.factual.driver.Query;
import com.factual.driver.ReadResponse;
import pt.isec.fd.internal.FactualDataAccessReader;
import pt.isec.msh.LogWriter;
import pt.isec.msh.MSClient;

import java.sql.SQLException;

/**
 * Created by Rui on 18/02/2016.
 */
public class FD_FactualReader {


    public static void ReadData(MSClient MySqlClient) throws SQLException {

        LogWriter.WriteTimeLog("START READ FACTUAL DATA");
        long startTime = System.currentTimeMillis();

        FactualDataAccessReader rdr = new FactualDataAccessReader(MySqlClient);
        rdr.GetAllData();
        long estimatedTime = System.currentTimeMillis() - startTime;
        LogWriter.WriteTimeLog("END READ FACTUAL DATA");
        LogWriter.WriteTimeLog("ELAPSED TIME " + estimatedTime);
    }

    public static void ContinueRead(MSClient MySqlClient, String StartValue) throws SQLException {
        FactualDataAccessReader rdr = new FactualDataAccessReader(MySqlClient);
        rdr.ContinueData(StartValue);
    }

    public static void CreateDatabase(MSClient MySqlClient) throws SQLException {

        StringBuilder _sb;
        if (!MySqlClient.TableExists(MSClient.table_prefix + "_FactualData")) {

            _sb = new StringBuilder();
            _sb.append("CREATE TABLE " + MSClient.table_prefix + "_FactualData( ");
            _sb.append("factual_id VARCHAR(50) NOT NULL, ");
            _sb.append("name VARCHAR(250) NULL,");
            _sb.append("tel VARCHAR(250) NULL,");
            _sb.append("locality VARCHAR(250) NULL,");
            _sb.append("region VARCHAR(250) NULL,");
            _sb.append("latitude DECIMAL(18,14) NULL,");
            _sb.append("longitude DECIMAL(18,14) NULL,");
            _sb.append("category_ids VARCHAR(250) NULL,");
            _sb.append("category_labels TEXT NULL,");
            _sb.append("postcode VARCHAR(50) NULL,");
            _sb.append("address TEXT  NULL,");
            _sb.append("address_extended TEXT  NULL,");
            _sb.append("website VARCHAR(2000) NULL,");
            _sb.append("hours VARCHAR(2000) NULL,");
            _sb.append("neighborhood VARCHAR(2000) NULL,");
            _sb.append("email TEXT NULL,");
            _sb.append("chain_id VARCHAR(50) NULL,");
            _sb.append("chain_name TEXT  NULL,");
            _sb.append("name_soundex VARCHAR(50) NULL,");
            _sb.append("PRIMARY KEY (factual_id) ");
            _sb.append(")");
            MySqlClient.ExecuteQuery(_sb.toString());
        }


        if (!MySqlClient.TableExists(MSClient.table_prefix + "_FactualDataCategory")) {
            _sb = new StringBuilder();
            _sb.append("CREATE TABLE " + MSClient.table_prefix + "_FactualDataCategory( ");
            _sb.append("factual_id VARCHAR(50) NOT NULL, ");
            _sb.append("category_id INT NOT NULL,");
            _sb.append("PRIMARY KEY (factual_id, category_id) ");
            _sb.append(")");

            MySqlClient.ExecuteQuery(_sb.toString());
        }

        if (!MySqlClient.TableExists(MSClient.table_prefix + "_FactualCategory")) {
            _sb = new StringBuilder();
            _sb.append("CREATE TABLE " + MSClient.table_prefix + "_FactualCategory( ");
            _sb.append("category_id INT NOT NULL,");
            _sb.append("parents INT NOT NULL, ");
            _sb.append("en VARCHAR(100) NOT NULL, ");
            _sb.append("pt VARCHAR(100) NOT NULL, ");
            _sb.append("base_category_id INT NOT NULL, ");
            _sb.append("PRIMARY KEY (category_id) ");
            _sb.append(")");

            MySqlClient.ExecuteQuery(_sb.toString());
        }

        if (!MySqlClient.TableExists(MSClient.table_prefix + "_FactualCrosswalk")) {
            _sb = new StringBuilder();
            _sb.append("CREATE TABLE " + MSClient.table_prefix + "_FactualCrosswalk ( ");
            _sb.append("factual_id VARCHAR(50) NOT NULL, ");
            _sb.append("url VARCHAR(500) NOT NULL, ");
            _sb.append("namespace VARCHAR(50) NOT NULL, ");
            _sb.append("article VARCHAR(100) NULL, ");
            _sb.append("PRIMARY KEY (factual_id, namespace, url) ");
            _sb.append(")");

            MySqlClient.ExecuteQuery(_sb.toString());
        }
    }

    public static void ClearData(MSClient MySqlClient) {
    }

    public static void ReadFactualCategories(MSClient mysql) {

        LogWriter.WriteTimeLog("START READ FACTUAL CATEGORIES");
        long startTime = System.currentTimeMillis();

        FactualDataAccessReader da = new FactualDataAccessReader(mysql);
        da.GetCategories();

        long estimatedTime = System.currentTimeMillis() - startTime;
        LogWriter.WriteTimeLog("END READ FACTUAL CATEGORIES");
        LogWriter.WriteTimeLog("ELAPSED TIME " + estimatedTime);

        //TODO: CRIAR TABELA PARA CATEGORIAS

        //TODO: LER CATEGORIAS DO FATUAL

        //TODO: ESCREVER NA BD

        //TODO: TENTAR LER CÓDIGOS AMERICANOS

        //TODO: PARA CADA CLASSE DEFINIR A CLASSE RAIZ

        //TODO: SEPARAR CATEGORIAS DOS POI'S
    }

    public static void ReadFactualCrossWalk(MSClient mysql,String StartID) throws SQLException {
        LogWriter.WriteTimeLog("START READ FACTUAL CROSSWALK ID:" + StartID);
        long startTime = System.currentTimeMillis();

        FactualDataAccessReader da = new FactualDataAccessReader(mysql);
        da.GetFactualCrossWalk(StartID);
        long estimatedTime = System.currentTimeMillis() - startTime;

        LogWriter.WriteTimeLog("END READ FACTUAL CROSSWALK ID:" + StartID);
        LogWriter.WriteTimeLog("ELAPSED TIME " + estimatedTime);
    }


}
