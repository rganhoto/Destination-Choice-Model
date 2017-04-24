package pt.isec.fc;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import pt.isec.fc.internal.jCluster;
import pt.isec.fc.internal.jName;
import pt.isec.fc.internal.jPoi;
import pt.isec.msh.LogWriter;
import pt.isec.msh.MSClient;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by Rui on 20/02/2016.
 */
public class FC_ExportCityCluster {

    public static void ExportSimpleClusterDataFiles(MSClient mysql) throws SQLException {
        try {
            ExportSimpleClusterDataFile(1, mysql);
            ExportSimpleClusterDataFile(2, mysql);
            ExportSimpleClusterDataFile(3, mysql);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void ExportSimpleClusterDataFile(int Zoom, MSClient mysql) throws FileNotFoundException, SQLException {
        LogWriter.WriteTimeLog("START EXPORT CLUSTER FILE Zoom" + Integer.toString(Zoom));
        long startTime = System.currentTimeMillis();

        FC_ConfigData _cfg = FC_ConfigData.Get();

        //OBTER NR DE CLUSTERS
        ArrayList<jCluster> Clusters = new ArrayList<jCluster>();


        //SELECT MAX clusterID from xZOOM1;

        int MaxClusterId = 0;

        String _query = "SELECT MAX(cluster_id) cluster_id " +
                "FROM " + mysql.table_prefix + "_CC_Zoom" + Zoom;

        ResultSet rsc = mysql.getData(_query);
        if (rsc.first()) {
            MaxClusterId = rsc.getInt("cluster_id");
        }
        rsc.close();

        for (int i = 0; i <= MaxClusterId; i++) {
            jCluster c = new jCluster();
            c.setId("cluster" + i);

            //OBTER POIS
            _query = "SELECT F.latitude, F.longitude, F.name, F.category_labels " +
                    "FROM " + mysql.table_prefix + "_FactualData F " +
                    "INNER JOIN " + mysql.table_prefix + "_CC_Zoom" + Zoom + " C ON C.factual_id=F.factual_id " +
                    "WHERE C.cluster_id=" + i;

            //NOMES

            double sumlat = 0;
            double sumlon = 0;

            Dictionary<String, jName> _dicCategories = new Hashtable<String, jName>();

            ResultSet rs = mysql.getData(_query);
            while (rs.next()) {
                jPoi p = new jPoi();
                p.setLon(rs.getDouble("longitude"));
                p.setLat(rs.getDouble("latitude"));
                p.setName(rs.getString("name"));
                p.setSource("factual");

                c.pois.add(p);

                sumlat += p.getLat();
                sumlon += p.getLon();

                //CATEGORIAS

                String _categoria = rs.getString("category_labels");
                if (_categoria == null || _categoria.equals(""))
                    _categoria = "UNDEFINED_CATEGORY";

                if (_dicCategories.get(_categoria) == null) {
                    jName _jn = new jName();
                    _jn.setName(_categoria);
                    _jn.Contagem = 1;

                    _dicCategories.put(_categoria, _jn);
                } else {
                    _dicCategories.get(_categoria).Contagem += 1;
                }
            }

            rs.close();

            //CIRAR OS CATEGORIAS

            Enumeration<jName> _cats = _dicCategories.elements();
            while (_cats.hasMoreElements()) {
                jName _c = _cats.nextElement();

                double peso = ((double) _c.Contagem / c.pois.size());

                jName n = new jName();
                n.setName(_c.getName());
                n.setWeight(Double.toString(peso));
                n.Contagem = _c.Contagem;

                c.name_weights.add(n);
            }

//ORDENAR LISTA
            Collections.sort(c.name_weights, new Comparator<jName>() {
                @Override
                public int compare(jName o1, jName o2) {
                    return o2.Contagem - o1.Contagem;
                }
            });

            //c.name_weights.add()

            //ADICIONAR AOS CLUSTERS
            if (c.pois.size() > 0) {
                //CALCULAR CENTROIDE
                c.setLon(sumlon / c.pois.size());
                c.setLat(sumlat / c.pois.size());

                Clusters.add(c);
            }
        }


        if (Clusters.size() > 0) {
            Gson gson = new GsonBuilder().create();
            PrintWriter out = new PrintWriter(mysql.table_prefix + "_cluster_Z" + Zoom + ".json");
            out.print(gson.toJson(Clusters));
            out.flush();
            out.close();


            float lat = 0;
            float lon = 0;
            for (jCluster c : Clusters) {
                lat += c.getLat();
                lon += c.getLon();
            }

            lat = lat / Clusters.size();
            lon = lon / Clusters.size();

            int zoomLevel = 0;
            switch (Zoom) {
                case 1:
                    zoomLevel = 10;
                    break;
                case 2:
                    zoomLevel = 13;
                    break;
                case 3:
                    zoomLevel = 16;
                    break;
            }

            //RubberClusters rc = new RubberClusters();
            //rc.CreateShapeFile(mysql.table_prefix + "_cluster_Z" + Zoom + ".json", lat,lon,zoomLevel);

            String[] strData = new String[4];
            strData[0] = mysql.table_prefix + "_cluster_Z" + Zoom + ".json";
            strData[1] = Float.toString(lat);
            strData[2] = Float.toString(lon);
            strData[3] = Integer.toString(zoomLevel);
            //strData[3]=Integer.toString(zoomLevel);

            //RubberClusters.Run(strData);
        }
        long estimatedTime = System.currentTimeMillis() - startTime;
        LogWriter.WriteTimeLog("END EXPORT CLUSTER FILE Zoom" + Integer.toString(Zoom));
        LogWriter.WriteTimeLog("ELAPSED TIME " + estimatedTime);

    }

    public static void ExportCategoryClusterDataFiles(MSClient mysql) {
        try {
            ExportCategoryClusterDataFile(1, mysql);
            ExportCategoryClusterDataFile(2, mysql);
            ExportCategoryClusterDataFile(3, mysql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static ArrayList<jCluster> Clusters = null;
    private static int iClusterNumber = 0;

    public static void ExportCategoryClusterDataFile(final int Zoom, final MSClient mysql) throws FileNotFoundException, SQLException {
        LogWriter.WriteTimeLog("START EXPORT CATEGORY CLUSTER FILE Zoom" + Integer.toString(Zoom));
        long startTime = System.currentTimeMillis();

        FC_ConfigData _cfg = FC_ConfigData.Get();

        //OBTER NR DE CLUSTERS
        Clusters = new ArrayList<jCluster>();


        //SELECT MAX clusterID from xZOOM1;

        String _query = "SELECT MAX(cluster_id) cluster_id, base_category_id " +
                "FROM " + mysql.table_prefix + "_CC_CAT_Zoom" + Zoom + " " +
                "GROUP BY base_category_id ";

        ResultSet rsc = mysql.getData(_query);

ArrayList<Thread> _threadList = new ArrayList<Thread>();
        //   iClusterNumber = 0;


        while (rsc.next()) {


            int MaxClusterId = rsc.getInt("cluster_id");
            final int base_category_id = rsc.getInt("base_category_id");

            for (int i = 0; i <= MaxClusterId; i++) {

                final int ClusterID=i;
                Thread t = new Thread(new Runnable() {
                @Override
                public void run() {



                        jCluster c = new jCluster();

                        //OBTER POIS
                        String _query2 = "SELECT F.latitude, F.longitude, F.name, MAX(FC.en) category_labels " +
                                "FROM " + mysql.table_prefix + "_FactualData F " +
                                "INNER JOIN " + mysql.table_prefix + "_CC_CAT_Zoom" + Zoom + " C ON C.factual_id=F.factual_id AND C.base_category_id = " + base_category_id + " " +
                                "INNER JOIN " + mysql.table_prefix + "_FactualCategory FC ON FC.base_category_id=C.base_category_id " +
                                "INNER JOIN " + mysql.table_prefix + "_FactualDataCategory FD ON FD.factual_id=F.factual_id AND FD.category_id= FC.category_id " +
                                "WHERE C.cluster_id=" + ClusterID +
                                " GROUP BY F.latitude, F.longitude, F.name ";

                        //NOMES

                        double sumlat = 0;
                        double sumlon = 0;

                        Dictionary<String, jName> _dicCategories = new Hashtable<String, jName>();

                        try {
                            ResultSet rs = mysql.getData(_query2);
                            while (rs.next()) {
                                jPoi p = new jPoi();
                                p.setLon(rs.getDouble("longitude"));
                                p.setLat(rs.getDouble("latitude"));
                                p.setName(rs.getString("name"));
                                p.setSource("factual");

                                c.pois.add(p);

                                sumlat += p.getLat();
                                sumlon += p.getLon();

                                //CATEGORIAS

                                String _categoria = rs.getString("category_labels");
                                if (_categoria == null || _categoria.equals(""))
                                    _categoria = "UNDEFINED_CATEGORY";

                                if (_dicCategories.get(_categoria) == null) {
                                    jName _jn = new jName();
                                    _jn.setName(_categoria);
                                    _jn.Contagem = 1;

                                    _dicCategories.put(_categoria, _jn);
                                } else {
                                    _dicCategories.get(_categoria).Contagem += 1;
                                }
                            }

                            rs.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }


                        //CIRAR OS CATEGORIAS

                        Enumeration<jName> _cats = _dicCategories.elements();
                        while (_cats.hasMoreElements()) {
                            jName _c = _cats.nextElement();

                            double peso = ((double) _c.Contagem / c.pois.size());

                            jName n = new jName();
                            n.setName(_c.getName());
                            n.setWeight(Double.toString(peso));
                            n.Contagem = _c.Contagem;

                            c.name_weights.add(n);
                        }

//ORDENAR LISTA
                        Collections.sort(c.name_weights, new Comparator<jName>() {
                            @Override
                            public int compare(jName o1, jName o2) {
                                return o2.Contagem - o1.Contagem;
                            }
                        });

                        //c.name_weights.add()

                        //ADICIONAR AOS CLUSTERS
                        if (c.pois.size() > 0) {
                            //CALCULAR CENTROIDE
                            c.setLon(sumlon / c.pois.size());
                            c.setLat(sumlat / c.pois.size());

                            c.setId("cluster" + Clusters.size());
                            Clusters.add(c);
                            System.out.println(Clusters.size());
                            iClusterNumber++;
                        }
                    }

            });
            t.start();
            _threadList.add(t);
System.out.print(".");

            }


        }

        while (_threadList.size()>0)
        {
            try {
                _threadList.get(0).join();
                _threadList.remove(0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        rsc.close();


        if (Clusters.size() > 0) {
            Gson gson = new GsonBuilder().create();
            PrintWriter out = new PrintWriter(mysql.table_prefix + "_cat_cluster_Z" + Zoom + ".json");
            out.print(gson.toJson(Clusters));
            out.flush();
            out.close();


            float lat = 0;
            float lon = 0;
            for (jCluster c : Clusters) {
                lat += c.getLat();
                lon += c.getLon();
            }

            lat = lat / Clusters.size();
            lon = lon / Clusters.size();

            int zoomLevel = 0;
            switch (Zoom) {
                case 1:
                    zoomLevel = 10;
                    break;
                case 2:
                    zoomLevel = 13;
                    break;
                case 3:
                    zoomLevel = 16;
                    break;
            }

            //RubberClusters rc = new RubberClusters();
            //rc.CreateShapeFile(mysql.table_prefix + "_cluster_Z" + Zoom + ".json", lat,lon,zoomLevel);

            String[] strData = new String[4];
            strData[0] = mysql.table_prefix + "_cat_cluster_Z" + Zoom + ".json";
            strData[1] = Float.toString(lat);
            strData[2] = Float.toString(lon);
            strData[3] = Integer.toString(zoomLevel);
            //strData[3]=Integer.toString(zoomLevel);


            PrintWriter _pw = null;
            try {
                _pw = new PrintWriter(new BufferedWriter(new FileWriter(mysql.table_prefix + "_cat_cluster.bat", true)));
                _pw.println("echo start " + Zoom + " %date% %time% >> rubbertime.txt");
                _pw.print("java -jar RubberClusters.jar ");
                _pw.print(mysql.table_prefix + "_cat_cluster_Z" + Zoom + ".json ");
                _pw.print(lat);
                _pw.print(" ");
                _pw.print(lon);
                _pw.print(" " + zoomLevel);

                _pw.println();
                _pw.print("REM map.center({lat: " + lat + ", lon: " + lon + "});");
                _pw.println();

                _pw.println("echo end " + Zoom + " %date% %time% >> rubbertime.txt");

                _pw.close();
                _pw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            //RubberClusters.Run(strData);
        }


        long estimatedTime = System.currentTimeMillis() - startTime;
        LogWriter.WriteTimeLog("END EXPORT CATEGORY CLUSTER FILE Zoom" + Integer.toString(Zoom));
        LogWriter.WriteTimeLog("ELAPSED TIME " + estimatedTime);

    }


}
