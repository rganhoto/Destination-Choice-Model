package pt.isec.fc;

import com.sun.xml.internal.bind.util.Which;
import pt.isec.fc.internal.ClusterGeneration;
import pt.isec.fc.internal.InstanceData;
import pt.isec.fc.internal.eNeighborData;
import pt.isec.msh.LogWriter;
import pt.isec.msh.MSClient;
import pt.isec.msh.MS_StatementHlp;
import weka.clusterers.SimpleKMeans;
import weka.core.Instances;
import weka.core.converters.ConverterUtils;
import weka.filters.unsupervised.attribute.ClusterMembership;
import weka.gui.beans.DataSource;
import weka.gui.explorer.ClustererPanel;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Created by Rui on 20/02/2016.
 */
public class FC_GenerateCluster {

    public static void CreateTables(MSClient cli) throws SQLException {
        FC_ConfigData _cfg = FC_ConfigData.Get();

        for (int i = 1; i <= 3; i++) {
            StringBuilder _sb;
            if (!cli.TableExists(cli.table_prefix + "_CC_Zoom" + i)) {
                _sb = new StringBuilder();
                _sb.append("CREATE TABLE " + cli.table_prefix + "_CC_Zoom" + i + " ( ");
                _sb.append("factual_id VARCHAR(50) NOT NULL, ");
                _sb.append("cluster_id INT NOT NULL, ");
                _sb.append("PRIMARY KEY (factual_id) ");
                _sb.append(")");

                cli.ExecuteQuery(_sb.toString());
            }

            if (!cli.TableExists(cli.table_prefix + "_CC_CAT_Zoom" + i)) {
                _sb = new StringBuilder();
                _sb.append("CREATE TABLE " + cli.table_prefix + "_CC_CAT_Zoom" + i + " ( ");
                _sb.append("factual_id VARCHAR(50) NOT NULL, ");
                _sb.append("base_category_id INT NOT NULL, ");
                _sb.append("cluster_id INT NOT NULL, ");
                _sb.append("PRIMARY KEY (factual_id, base_category_id) ");
                _sb.append(")");

                cli.ExecuteQuery(_sb.toString());
            }

            if (!cli.TableExists(cli.table_prefix + "_FactualNeighbor")) {
                _sb = new StringBuilder();
                _sb.append("CREATE TABLE " + cli.table_prefix + "_FactualNeighbor ( ");
                _sb.append("factual_id VARCHAR(50) NOT NULL, ");
                _sb.append("NNeighbor int NOT NULL, ");
                _sb.append("base_category_id int NOT NULL, ");
                _sb.append("distance decimal(18,14), ");
                _sb.append("PRIMARY KEY (factual_id, NNeighbor, base_category_id) ");
                _sb.append(")");

                cli.ExecuteQuery(_sb.toString());
            }

            if (!cli.TableExists(cli.table_prefix + "_FactualNearPoint")) {
                _sb = new StringBuilder();
                _sb.append("CREATE TABLE " + cli.table_prefix + "_FactualNearPoint ( ");
                _sb.append("factual_id VARCHAR(50) NOT NULL PRIMARY KEY, ");
                _sb.append("pointcount int ");
                _sb.append(")");

                cli.ExecuteQuery(_sb.toString());
            }

        }


    }

    public static void ClearData(MSClient mscli) {
    }

    public static void GetNearestNeighbor(MSClient mscli, int NNeighbor) throws Exception {
        FC_ConfigData cfg = FC_ConfigData.Get();

        String strSQL = "DELETE FROM " + MSClient.table_prefix + "_FactualNeighbor WHERE NNeighbor=" + NNeighbor;
        mscli.ExecuteQuery(strSQL);

        LogWriter.WriteTimeLog("START NEAREST NEIGHBOR");


        strSQL = "SELECT DISTINCT base_category_id from " + MSClient.table_prefix + "_FactualCategory";
        ResultSet rsCat = mscli.getData(strSQL);

        while (rsCat.next()) {


            strSQL = "SELECT DISTINCT FD.factual_id, FD.Latitude, FD.Longitude " +
                    "FROM " + MSClient.table_prefix + "_FactualData FD " +
                    "INNER JOIN " + MSClient.table_prefix + "_FactualDataCategory FCD ON FCD.factual_id = FD.factual_id " +
                    "INNER JOIN " + MSClient.table_prefix + "_FactualCategory C ON C.category_id = FCD.category_id AND C.base_category_id = " + rsCat.getInt("base_category_id") + " " +
                    "WHERE FD.Latitude IS NOT NULL ";
            ArrayList<eNeighborData> _lstData = new ArrayList<eNeighborData>();

            ResultSet rs = mscli.getData(strSQL);
            while (rs.next()) {
                eNeighborData _data = new eNeighborData();
                _data.factual_id = rs.getString("factual_id");
                _data.Latitude = rs.getDouble("Latitude");
                _data.Longitude = rs.getDouble("Longitude");

                _lstData.add(_data);
            }
            rs.close();

            if (_lstData.size() >= NNeighbor) {

                for (int i = 0; i < _lstData.size(); i++) {
                    for (int j = i + 1; j < _lstData.size(); j++) {
                        _lstData.get(i).SetMinDistance(_lstData.get(j), NNeighbor);
                    }
                }

                strSQL = "REPLACE INTO " + MSClient.table_prefix + "_FactualNeighbor (factual_id, NNeighbor, base_category_id, distance) " +
                        "VALUES(?,?,?,?) ";

                PreparedStatement stmt = mscli.PrepareStatement(strSQL);

                for (eNeighborData nb : _lstData) {
                    MS_StatementHlp.SetString(stmt, 1, nb.factual_id);
                    MS_StatementHlp.SetInt(stmt, 2, NNeighbor);
                    MS_StatementHlp.SetInt(stmt, 3, rsCat.getInt("base_category_id"));
                    MS_StatementHlp.SetDecimal2(stmt, 4, nb.LastDistance);
                    stmt.executeUpdate();
                }
                mscli.FinishStatement();
                System.out.println(".");
            }
        }
        rsCat.close();
        LogWriter.WriteTimeLog("FINISH NEAREST NEIGHBOR");
    }


    public static void NumberPointsNearNeighbor(MSClient mscli, double epsilon) throws Exception {
        FC_ConfigData cfg = FC_ConfigData.Get();

        String strSQL = "DELETE FROM " + MSClient.table_prefix + "_FactualNearPoint";
        mscli.ExecuteQuery(strSQL);

        LogWriter.WriteTimeLog("START READ NEAREST POINTS");

        strSQL = "SELECT factual_id, Latitude, Longitude FROM " + MSClient.table_prefix + "_FactualData WHERE Latitude IS NOT NULL ";
        ArrayList<eNeighborData> _lstData = new ArrayList<eNeighborData>();


        ResultSet rs = mscli.getData(strSQL);
        while (rs.next()) {
            eNeighborData _data = new eNeighborData();
            _data.factual_id = rs.getString("factual_id");
            _data.Latitude = rs.getDouble("Latitude");
            _data.Longitude = rs.getDouble("Longitude");

            _lstData.add(_data);
        }
        rs.close();

        LogWriter.WriteTimeLog("START CALC NEAREST POINTS");

        for (int i = 0; i < _lstData.size(); i++) {
            for (int j = i + 1; j < _lstData.size(); j++) {
                _lstData.get(i).IsInDistance(_lstData.get(j), epsilon);
            }
        }

        LogWriter.WriteTimeLog("START INSERT NEAREST POINTS");

        strSQL = "INSERT INTO " + MSClient.table_prefix + "_FactualNearPoint (factual_id, pointcount) " +
                "VALUES(?,?) ";

        PreparedStatement stmt = mscli.PrepareStatement(strSQL);

        for (eNeighborData nb : _lstData) {
            MS_StatementHlp.SetString(stmt, 1, nb.factual_id);
            MS_StatementHlp.SetInt(stmt, 2, nb.PointCount);
            stmt.executeUpdate();
        }
        mscli.FinishStatement();

        LogWriter.WriteTimeLog("FINISH NEAREST POINTS");

    }


    public static void CreateSimpleClusters(MSClient mscli) throws Exception {

        FC_ConfigData _cfg = FC_ConfigData.Get();

        ClusterGeneration cg = new ClusterGeneration(mscli);
        cg.ClearOldSimpleClusters();

        cg.GetFactualData();

        if (_cfg.cc_zoom1_minpoints > 0 && _cfg.cc_zoom1_epsilon > 0) {
            LogWriter.WriteTimeLog("START CREATE CLUSTER ZOOM 1");
            long startTime = System.currentTimeMillis();
            ArrayList<InstanceData> clusters = cg.ExecuteDBSCAN(_cfg.cc_zoom1_minpoints, _cfg.cc_zoom1_epsilon);
            cg.SaveZoomClusters(1, clusters);
            long estimatedTime = System.currentTimeMillis() - startTime;
            LogWriter.WriteTimeLog("END CREATE CLUSTER ZOOM 1");
            LogWriter.WriteTimeLog("ELAPSED TIME " + estimatedTime);
        }
        if (_cfg.cc_zoom2_minpoints > 0 && _cfg.cc_zoom2_epsilon > 0) {
            LogWriter.WriteTimeLog("START CREATE CLUSTER ZOOM 2");
            long startTime = System.currentTimeMillis();
            ArrayList<InstanceData> clusters = cg.ExecuteDBSCAN(_cfg.cc_zoom2_minpoints, _cfg.cc_zoom2_epsilon);
            cg.SaveZoomClusters(2, clusters);
            long estimatedTime = System.currentTimeMillis() - startTime;
            LogWriter.WriteTimeLog("END CREATE CLUSTER ZOOM 2");
            LogWriter.WriteTimeLog("ELAPSED TIME " + estimatedTime);
        }
        if (_cfg.cc_zoom3_minpoints > 0 && _cfg.cc_zoom3_epsilon > 0) {
            LogWriter.WriteTimeLog("START CREATE CLUSTER ZOOM 3");
            long startTime = System.currentTimeMillis();
            ArrayList<InstanceData> clusters = cg.ExecuteDBSCAN(_cfg.cc_zoom3_minpoints, _cfg.cc_zoom3_epsilon);
            cg.SaveZoomClusters(3, clusters);
            long estimatedTime = System.currentTimeMillis() - startTime;
            LogWriter.WriteTimeLog("END CREATE CLUSTER ZOOM 3");
            LogWriter.WriteTimeLog("ELAPSED TIME " + estimatedTime);
        }
    }

    public static void CreateCategoryClusters(MSClient mscli) throws Exception {
        FC_ConfigData _cfg = FC_ConfigData.Get();

        ClusterGeneration cg = new ClusterGeneration(mscli);
        cg.ClearOldCategoryClusters();


        ArrayList<Integer> lstBaseCategories = cg.GetBaseCategoryList();

        //TODO: SACAR LISTA DE BASE CATEGORY

        LogWriter.WriteTimeLog("START CREATE CATEGORY CLUSTER ZOOM 1");
        long startTime = System.currentTimeMillis();

        for (int baseCategory : lstBaseCategories) {

            cg.GetCategorizedData(baseCategory);

            if (cg._instancias.size() >= _cfg.cc_zoom1_minpoints) {
                if (_cfg.cc_zoom1_minpoints > 0 && _cfg.cc_zoom1_epsilon > 0) {
                    ArrayList<InstanceData> clusters = cg.ExecuteDBSCAN(_cfg.cc_zoom1_minpoints, _cfg.cc_zoom1_epsilon);
                    cg.SaveCategoryZoomClusters(1, baseCategory, clusters);
                }
            }
        }

        long estimatedTime = System.currentTimeMillis() - startTime;
        LogWriter.WriteTimeLog("END CREATE CATEGORY CLUSTER ZOOM 1");
        LogWriter.WriteTimeLog("ELAPSED TIME " + estimatedTime);

        LogWriter.WriteTimeLog("START CREATE CATEGORY CLUSTER ZOOM 2");
        startTime = System.currentTimeMillis();

        for (int baseCategory : lstBaseCategories) {
            cg.GetCategorizedData(baseCategory);

            if (cg._instancias.size() >= _cfg.cc_zoom2_minpoints) {
                if (_cfg.cc_zoom2_minpoints > 0 && _cfg.cc_zoom2_epsilon > 0) {
                    ArrayList<InstanceData> clusters = cg.ExecuteDBSCAN(_cfg.cc_zoom2_minpoints, _cfg.cc_zoom2_epsilon);
                    cg.SaveCategoryZoomClusters(2, baseCategory, clusters);
                }
            }
        }
        estimatedTime = System.currentTimeMillis() - startTime;
        LogWriter.WriteTimeLog("END CREATE CATEGORY CLUSTER ZOOM 2");
        LogWriter.WriteTimeLog("ELAPSED TIME " + estimatedTime);

        LogWriter.WriteTimeLog("START CREATE CATEGORY CLUSTER ZOOM 3");
        startTime = System.currentTimeMillis();

        for (int baseCategory : lstBaseCategories) {
            cg.GetCategorizedData(baseCategory);

            if (cg._instancias.size() >= _cfg.cc_zoom3_minpoints) {
                if (_cfg.cc_zoom3_minpoints > 0 && _cfg.cc_zoom3_epsilon > 0) {
                    ArrayList<InstanceData> clusters = cg.ExecuteDBSCAN(_cfg.cc_zoom3_minpoints, _cfg.cc_zoom3_epsilon);
                    cg.SaveCategoryZoomClusters(3, baseCategory, clusters);
                }
            }
        }
        estimatedTime = System.currentTimeMillis() - startTime;
        LogWriter.WriteTimeLog("END CREATE CATEGORY CLUSTER ZOOM 3");
        LogWriter.WriteTimeLog("ELAPSED TIME " + estimatedTime);

    }


    public static void KMEans() throws Exception {

        //http://stackoverflow.com/questions/23872807/get-cluster-assignments-in-weka

        Instances data = ConverterUtils.DataSource.read("test.csv");
        SimpleKMeans kmeans = new SimpleKMeans();
        kmeans.setNumClusters(100);
        kmeans.buildClusterer(data);

        int[] assignments = kmeans.getAssignments();
        int i = 0;
        for (int clusterNum : assignments) {
            //I = Indice da Instancia
            int id = (int) data.instance(i).value(0);
            System.out.printf("ID %d -> Cluster %d \n", id, assignments[i]);
            i++;
        }


        //DADOS NECESSARIOS PARA RECOLHA

        //TABELA 'PREFFIX'_CONIFG (CHAVE;VALOR) ;


    }


}
