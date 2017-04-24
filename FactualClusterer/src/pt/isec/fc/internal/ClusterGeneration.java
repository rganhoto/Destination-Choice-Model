package pt.isec.fc.internal;

import pt.isec.fc.FC_ConfigData;
import pt.isec.msh.MSClient;
import pt.isec.msh.MS_StatementHlp;
import weka.clusterers.DBSCAN;
import weka.core.*;
import weka.experiment.InstanceQuery;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by Rui on 20/02/2016.
 */
public class ClusterGeneration {

    MSClient _sqlcli;
    Attribute attLat = new Attribute("latitude");
    Attribute attLon = new Attribute("longitude");

    public ArrayList<InstanceData> _instancias;


    public ClusterGeneration(MSClient cli) {
        _sqlcli = cli;
    }

    public void GetFactualData() throws SQLException {
        String query = "SELECT factual_id, latitude, longitude FROM " + _sqlcli.table_prefix + "_FactualData " +
                "WHERE latitude IS NOT NULL AND longitude IS NOT NULL ";

        ResultSet rs = _sqlcli.getData(query);

        _instancias = new ArrayList<InstanceData>();

        while (rs.next()) {
            InstanceData id = new InstanceData();
            id.factual_id = rs.getString("factual_id");
            id.latitude = rs.getDouble("latitude");
            id.longitude = rs.getDouble("longitude");
            _instancias.add(id);
        }

        rs.close();
    }

    public void GetCategorizedData(int BaseCategory) throws SQLException {
        String query = "SELECT DISTINCT D.factual_id, D.latitude, D.longitude FROM " + _sqlcli.table_prefix + "_FactualData D " +
                "INNER JOIN " + _sqlcli.table_prefix + "_FactualDataCategory FD ON FD.factual_id=D.factual_id " +
                "INNER JOIN " + _sqlcli.table_prefix + "_FactualCategory FC ON FC.category_id = FD.category_id AND FC.base_category_id=" + BaseCategory + " " +
                "WHERE D.latitude IS NOT NULL AND D.longitude IS NOT NULL ";

        ResultSet rs = _sqlcli.getData(query);

        _instancias = new ArrayList<InstanceData>();

        while (rs.next()) {
            InstanceData id = new InstanceData();
            id.factual_id = rs.getString("factual_id");
            id.latitude = rs.getDouble("latitude");
            id.longitude = rs.getDouble("longitude");
            _instancias.add(id);
        }

        rs.close();
    }

    public ArrayList<InstanceData> ExecuteDBSCAN(int minPoints, double epsilon) throws Exception {

        //CRIAR AS INSTANCIAS

        ArrayList<Attribute> atts = new ArrayList<Attribute>();
        atts.add(attLat);
        atts.add(attLon);

        Instances weka_instances = new Instances("ClusterData", atts, 0);

        for (InstanceData id : _instancias) {
            Instance inst = new DenseInstance(2);
            inst.setValue(attLat, id.latitude);
            inst.setValue(attLon, id.longitude);
            inst.setDataset(weka_instances);
            weka_instances.add(inst);
        }

        DBSCAN c = new DBSCAN();
        c.setEpsilon(epsilon);
        c.setMinPoints(minPoints);
        c.setDatabase_Type("weka.clusterers.forOPTICSAndDBScan.Databases.SequentialDatabase");
        c.setDatabase_distanceType("weka.clusterers.forOPTICSAndDBScan.DataObjects.EuclideanDataObject");

        c.buildClusterer(weka_instances);

        int clustNumber = c.numberOfClusters();

        ArrayList<InstanceData> _lstClusters = new ArrayList<InstanceData>();
        for (int i = 0; i < weka_instances.numInstances(); i++) {
            try {
                Instance inst = weka_instances.instance(i);
                int ClusterID = c.clusterInstance(inst);
                //SE DER ERRO É PORQUE É NOISE
                InstanceData iData = _instancias.get(i);
                iData.cluster_id = ClusterID;
                _lstClusters.add(iData);
            } catch (Exception ex) {
                // ex.printStackTrace();
            }
        }

        return _lstClusters;
    }


    public void SaveZoomClusters(int Zoom, ArrayList<InstanceData> lstInstancias) {
        StringBuilder sb = new StringBuilder();
        sb.append("REPLACE INTO " + _sqlcli.table_prefix + "_CC_Zoom" + Zoom + " ");
        sb.append("(factual_id, cluster_id )");
        sb.append("VALUES (?, ? )");

        PreparedStatement stmt;
        try {
            stmt = _sqlcli.PrepareStatement(sb.toString());

            for (InstanceData id : lstInstancias) {
                try {

                    MS_StatementHlp.SetString(stmt, 1, id.factual_id);
                    MS_StatementHlp.SetInt(stmt, 2, id.cluster_id);

                    stmt.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            _sqlcli.FinishStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    public void SaveCategoryZoomClusters(int Zoom, int BaseCategory, ArrayList<InstanceData> lstInstancias) {
        StringBuilder sb = new StringBuilder();
        sb.append("REPLACE INTO " + _sqlcli.table_prefix + "_CC_CAT_Zoom" + Zoom + " ");
        sb.append("(factual_id, base_category_id, cluster_id )");
        sb.append("VALUES (?, ?, ? )");

        PreparedStatement stmt;
        try {
            stmt = _sqlcli.PrepareStatement(sb.toString());

            for (InstanceData id : lstInstancias) {
                try {

                    MS_StatementHlp.SetString(stmt, 1, id.factual_id);
                    MS_StatementHlp.SetInt(stmt, 2, BaseCategory);
                    MS_StatementHlp.SetInt(stmt, 3, id.cluster_id);

                    stmt.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            _sqlcli.FinishStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


    public void ClearOldSimpleClusters() throws SQLException {
        for (int Zoom = 1; Zoom <= 3; Zoom++) {
            String Query = "DELETE FROM " + _sqlcli.table_prefix + "_CC_Zoom" + Zoom + " ";
            _sqlcli.ExecuteQuery(Query);
        }
    }

    public void ClearOldCategoryClusters() throws SQLException {
        for (int Zoom = 1; Zoom <= 3; Zoom++) {
            String Query = "DELETE FROM " + _sqlcli.table_prefix + "_CC_CAT_Zoom" + Zoom + " ";
            _sqlcli.ExecuteQuery(Query);
        }
    }

    public ArrayList<Integer> GetBaseCategoryList() throws SQLException {
        ArrayList<Integer> _lst = new ArrayList<Integer>();

        String query = "select distinct base_category_id from " + _sqlcli.table_prefix + "_FactualCategory  ";

        ResultSet rs = _sqlcli.getData(query);

        _instancias = new ArrayList<InstanceData>();

        while (rs.next()) {
            _lst.add(rs.getInt("base_category_id"));
        }
        rs.close();
        return _lst;
    }

}
