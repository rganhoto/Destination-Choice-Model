package pt.isec.dbpr;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import pt.isec.msh.LogWriter;
import pt.isec.msh.MSClient;
import pt.isec.msh.MS_StatementHlp;

import javax.xml.transform.Result;
import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Created by Rui on 01/05/2016.
 */
public class WP_dbPediaDownloader {


    public static String dbpedia_word;
    public static String path;
    public static String file1;
    public static String file2;
    public static String file3;
    public static String file4;
    public static String file5;
    public static String file6;
    public static String file7;
    public static String file8;
    public static String file9;
    public static String file10;

    public static void DownloadDataFromDBPedia(MSClient mysql) throws InterruptedException, IOException, SQLException {
        WP_dbPediaDownloader _d = new WP_dbPediaDownloader();
        _d.DownloadFilesIfEmpty(mysql);
    }

    public static void GetDbpediaData_TEST(MSClient mysql) throws SQLException, IOException {
        WP_dbPediaDownloader _d = new WP_dbPediaDownloader();
        ArrayList<String> ArticleNames = _d.GetArticleNames(mysql);
        _d.GetDataFromArticles("_dbpedia_data/instance_types_dbtax_dbo_en.ttl", ArticleNames, mysql);
        _d.GetDataFromArticles("_dbpedia_data/homepages_en.ttl", ArticleNames, mysql);

        _d.CleanRawData(mysql);
    }


    //GEO COORDINATES
    //http://downloads.dbpedia.org/2015-10/core-i18n/en/geo_coordinates_en.ttl.bz2

    //INFOBOX PROPERTIES
    //http://downloads.dbpedia.org/2015-10/core-i18n/en/infobox_properties_en.ttl.bz2

    //instance types
    //http://downloads.dbpedia.org/2015-10/core-i18n/en/instance_types_en.ttl.bz2

    //External Links
    //http://downloads.dbpedia.org/2015-10/core-i18n/en/external_links_en.ttl.bz2


    public void DownloadFilesIfEmpty(MSClient mysql) throws InterruptedException, IOException, SQLException {
/*        Thread t1 = _downloadFileFromURL("http://downloads.dbpedia.org/2015-10/core-i18n/en/infobox_properties_en.ttl.bz2", "infobox_properties_en.ttl.bz2");

        if (t1 != null && t1.isAlive())
            t1.join();

        Thread t2 = _downloadFileFromURL("http://downloads.dbpedia.org/2015-10/core-i18n/en/article_categories_en.ttl.bz2", "article_categories_en.ttl.bz2");
        Thread t3 = _downloadFileFromURL("http://downloads.dbpedia.org/2015-10/core-i18n/en/instance_types_en.ttl.bz2", "instance_types_en.ttl.bz2");
        Thread t4 = _downloadFileFromURL("http://downloads.dbpedia.org/2015-10/core-i18n/en/instance_types_dbtax_dbo_en.ttl.bz2", "instance_types_dbtax_dbo_en.ttl.bz2");
        Thread t5 = _downloadFileFromURL("http://downloads.dbpedia.org/2015-10/core-i18n/en/homepages_en.ttl.bz2", "homepages_en.ttl.bz2");
        //http://downloads.dbpedia.org/2015-10/core-i18n/en/homepages_en.ttl.bz2

        //Thread t2 = _downloadFileFromURL("http://downloads.dbpedia.org/2015-10/core-i18n/en/geo_coordinates_en.ttl.bz2", "geo_coordinates_en.ttl.bz2");
        //Thread t4 = _downloadFileFromURL("http://downloads.dbpedia.org/2015-10/core-i18n/en/external_links_en.ttl.bz2", "external_links_en.ttl.bz2");

        ArrayList<String> ArticleNames = GetArticlesContainingWord("_dbpedia_data/infobox_properties_en.ttl", dbpedia_word, mysql);

        if (t2 != null && t2.isAlive())
            t2.join();
        GetDataFromArticles("_dbpedia_data/article_categories_en.ttl", ArticleNames, mysql);

        if (t3 != null && t3.isAlive())
            t3.join();
        GetDataFromArticles("_dbpedia_data/instance_types_en.ttl", ArticleNames, mysql);


        if (t4 != null && t4.isAlive())
            t4.join();
        GetDataFromArticles("_dbpedia_data/instance_types_dbtax_dbo_en.ttl", ArticleNames, mysql);

        if (t5 != null && t5.isAlive())
            t5.join();
        GetDataFromArticles("_dbpedia_data/homepages_en.ttl", ArticleNames, mysql);
*/


        ArrayList<String> ArticleNames = GetArticlesContainingWord(GetPath(path, file1), dbpedia_word, mysql);
        if (file2 != null && file2.length() > 2)
            GetDataFromArticles(GetPath(path, file2), ArticleNames, mysql);
        if (file3 != null && file3.length() > 2)
            GetDataFromArticles(GetPath(path, file3), ArticleNames, mysql);
        if (file4 != null && file4.length() > 2)
            GetDataFromArticles(GetPath(path, file4), ArticleNames, mysql);
        if (file5 != null && file5.length() > 2)
            GetDataFromArticles(GetPath(path, file5), ArticleNames, mysql);
        if (file6 != null && file6.length() > 2)
            GetDataFromArticles(GetPath(path, file6), ArticleNames, mysql);
        if (file7 != null && file7.length() > 2)
            GetDataFromArticles(GetPath(path, file7), ArticleNames, mysql);
        if (file8 != null && file8.length() > 2)
            GetDataFromArticles(GetPath(path, file8), ArticleNames, mysql);
        if (file9 != null && file9.length() > 2)
            GetDataFromArticles(GetPath(path, file9), ArticleNames, mysql);
        if (file10 != null && file10.length() > 2)
            GetDataFromArticles(GetPath(path, file10), ArticleNames, mysql);
        CleanRawData(mysql);
    }

    private String GetPath(String path, String file) {
        if (path.length() > 0)
            return path + "/" + file;
        return file;
    }

    private Thread _downloadFileFromURL(String urlString, final String destination) {
        Thread _t = null;
        try {


            System.out.println("Downloading " + destination);
            File theDir = new File("_dbpedia_data");
            if (!theDir.exists()) {
                try {
                    theDir.mkdir();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            final String destinationURI = "_dbpedia_data/" + destination;

            File f = new File(destinationURI);
            if (!f.exists()) {
                URL website = new URL(urlString);
                ReadableByteChannel rbc;
                rbc = Channels.newChannel(website.openStream());
                FileOutputStream fos = new FileOutputStream(destinationURI);
                fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
                fos.close();
                rbc.close();
            }


            _t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        _extractFile(destinationURI, destinationURI.replace(".bz2", ""));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            _t.start();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return _t;
    }

    private void _extractFile(String fileToExtract, String DestinationFile) throws IOException {


        File f = new File(DestinationFile);
        if (!f.exists()) {

            try {
                Process p = Runtime.getRuntime().exec("_dbpedia_data/bzip2 -d -k " + fileToExtract.replace("_dbpedia_data\\", ""));
                p.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();


                final int buffersize = 1024;
                FileInputStream in = new FileInputStream(fileToExtract);
                FileOutputStream out = new FileOutputStream(DestinationFile);
                BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(in);
                final byte[] buffer = new byte[buffersize];

                int n;
                while (-1 != (n = bzIn.read(buffer))) {
                    out.write(buffer, 0, n);
                }

                out.close();
                bzIn.close();
            }
        }

    }

    private ArrayList<String> GetArticlesContainingWord(String fileToRead, String Word, MSClient mysql) throws IOException {

        LogWriter.WriteTimeLog("START ARTICLES CONTAINING WORD");
        long startTime = System.currentTimeMillis();

        String UpperCaseWord = Word.toUpperCase();
        ArrayList<String> _ArticleNames = new ArrayList<String>();

        //ArrayList<TurtleTripplet> FinalArticleData = new ArrayList<TurtleTripplet>();

        String CurrentArticleName = "";
        ArrayList<TurtleTripplet> CurrentArticleData = new ArrayList<TurtleTripplet>();
        boolean AddArticle = false;

        //ArrayList<String> ArticleNames = new ArrayList<String>();

        FileReader fr = new FileReader(fileToRead);
        BufferedReader br = new BufferedReader(fr);
        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#"))
                continue;

            TurtleTripplet ttl = ParseTripplet(line);

            if (!CurrentArticleName.equals(ttl.Name)) {
                if (AddArticle) {
                    _ArticleNames.add(CurrentArticleName);
                    SaveArticleData(CurrentArticleData, mysql);
                }//FinalArticleData.addAll(CurrentArticleData);
                CurrentArticleName = ttl.Name;
                AddArticle = false;
                CurrentArticleData.clear();
            }

            CurrentArticleData.add(ttl);

            // process the line.
            if (line.toUpperCase().contains(UpperCaseWord)) {
                AddArticle = true;
            }
        }

        if (AddArticle) {
            _ArticleNames.add(CurrentArticleName);
            SaveArticleData(CurrentArticleData, mysql);
        }

        long estimatedTime = System.currentTimeMillis() - startTime;
        LogWriter.WriteTimeLog("END ARTICLES CONTAINING WORD");
        LogWriter.WriteTimeLog("ELAPSED TIME " + estimatedTime);
        br.close();
        fr.close();
        return _ArticleNames;
    }

    private void SaveArticleData(ArrayList<TurtleTripplet> Data, MSClient mysql) {
        //TODO: GRAVAR NO MYSQL
        StringBuilder _sb = new StringBuilder();

        _sb.append("INSERT INTO " + MSClient.table_prefix + "_dbpediaRawData");
        _sb.append("(name, property, value )");
        _sb.append("VALUES (?, ?, ?)");

        PreparedStatement stmt = null;

        try {
            stmt = mysql.PrepareStatement(_sb.toString());

            for (TurtleTripplet ttl : Data) {
                MS_StatementHlp.SetString(stmt, 1, ttl.Name);
                MS_StatementHlp.SetString(stmt, 2, ttl.Property);
                MS_StatementHlp.SetString(stmt, 3, ttl.Value);
                stmt.executeUpdate();
            }

            mysql.FinishStatement();

            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            LogWriter.WriteErrorLogs(e);
        }


    }

    private void GetDataFromArticles(String fileToRead, ArrayList<String> ArticleNames, MSClient mysql) throws IOException {

        LogWriter.WriteTimeLog("START GET DATA FROM DUMP " + fileToRead);
        long startTime = System.currentTimeMillis();

        FileReader fr = new FileReader(fileToRead);
        BufferedReader br = new BufferedReader(fr);
        String line;

        ArrayList<TurtleTripplet> _listTTL = new ArrayList<TurtleTripplet>();

        String _lastArticle = "";
        boolean _isToAdd = false;

        while ((line = br.readLine()) != null) {
            if (line.startsWith("#"))
                continue;

            TurtleTripplet _ttl = ParseTripplet(line);
            if (!_lastArticle.equals(_ttl.Name)) {
                if (_isToAdd) {
                    SaveArticleData(_listTTL, mysql);
                }

                _listTTL = new ArrayList<TurtleTripplet>();
                _isToAdd = false;
                _lastArticle = _ttl.Name;
                if (ArticleNames.contains(_ttl.Name)) {
                    _listTTL.add(_ttl);
                    _isToAdd = true;
                }
            } else if (_isToAdd)
                _listTTL.add(_ttl);
        }

        if (_isToAdd)
            SaveArticleData(_listTTL, mysql);

        br.close();
        fr.close();

        long estimatedTime = System.currentTimeMillis() - startTime;
        LogWriter.WriteTimeLog("END GET DATA FROM DUMP " + fileToRead);
        LogWriter.WriteTimeLog("ELAPSED TIME " + estimatedTime);
    }

    private void GetCategoriesFromArticles(String fileToRead, ArrayList<String> ArticleNames) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(fileToRead));
        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#"))
                continue;

            TurtleTripplet _ttl = ParseTripplet(line);
            if (ArticleNames.contains(_ttl.Name)) {
                //GET DATA
                //INSERT INTO XPTO
            }
        }
    }

    private TurtleTripplet ParseTripplet(String DumpLine) {

        TurtleTripplet ttl = new TurtleTripplet();
        int iEspaco = DumpLine.indexOf(' ');

        ttl.Name = DumpLine.substring(0, iEspaco);

        DumpLine = DumpLine.substring(iEspaco + 1);

        iEspaco = DumpLine.indexOf(' ');

        ttl.Property = DumpLine.substring(0, iEspaco);

        DumpLine = DumpLine.substring(iEspaco + 1);

        if (DumpLine.endsWith(" ."))
            DumpLine = DumpLine.substring(0, DumpLine.length() - 2);

        ttl.Value = DumpLine;

        return ttl;
    }

    public static void CreateDataBase(MSClient mysql) throws SQLException {
        StringBuilder _sb = null;

        if (!mysql.TableExists(MSClient.table_prefix + "_dbpediaRawData")) {
            _sb = new StringBuilder();
            _sb.append("CREATE TABLE " + MSClient.table_prefix + "_dbpediaRawData( ");
            _sb.append("ID BIGINT NOT NULL AUTO_INCREMENT, ");
            _sb.append("name VARCHAR(200) NOT NULL, ");
            _sb.append("property VARCHAR(200) NOT NULL, ");
            _sb.append("value TEXT NOT NULL, ");
            _sb.append("clean_name VARCHAR(200) NULL, ");
            _sb.append("clean_value TEXT NULL, ");
            _sb.append("clean_datatype VARCHAR(200) NULL, ");
            _sb.append("PRIMARY KEY (ID) ");
            _sb.append(")");

            mysql.ExecuteQuery(_sb.toString());

            //CREATE INDEXES
            mysql.ExecuteQuery("CREATE INDEX iRawCLName ON " + MSClient.table_prefix + "_dbpediaRawData (clean_name)");
            mysql.ExecuteQuery("CREATE INDEX iRawName ON " + MSClient.table_prefix + "_dbpediaRawData (name)");
            mysql.ExecuteQuery("CREATE INDEX iRawProp ON " + MSClient.table_prefix + "_dbpediaRawData (property)");
            mysql.ExecuteQuery("CREATE INDEX iRawNameProp ON " + MSClient.table_prefix + "_dbpediaRawData (name,property)");
            mysql.ExecuteQuery("CREATE INDEX iRawCLNameProp ON " + MSClient.table_prefix + "_dbpediaRawData (clean_name,property)");

        }


        if (!mysql.TableExists(MSClient.table_prefix + "_dbpediaData")) {
            _sb = new StringBuilder();
            _sb.append("CREATE TABLE " + MSClient.table_prefix + "_dbpediaData( ");
            _sb.append("article VARCHAR(200) NOT NULL, ");
            _sb.append("name VARCHAR(200) NULL, ");
            _sb.append("website VARCHAR(500) NULL, ");
            _sb.append("location VARCHAR(500) NULL, ");
            _sb.append("address TEXT NULL, ");
            _sb.append("category VARCHAR(2000) NULL, ");
            _sb.append("latitude DECIMAL(18,10) NULL, ");
            _sb.append("longitude DECIMAL(18,10) NULL, ");
            _sb.append("name_soundex VARCHAR(2000) NULL, ");
            _sb.append("");
            _sb.append("PRIMARY KEY (article) ");
            _sb.append(")");
            mysql.ExecuteQuery(_sb.toString());
        }
    }

    private ArrayList<String> GetArticleNames(MSClient mysql) throws SQLException {
        ArrayList<String> _lstArticles = new ArrayList<String>();

        StringBuilder _sb = new StringBuilder();
        _sb.append("SELECT DISTINCT name FROM " + MSClient.table_prefix + "_dbpediaRawData");

        ResultSet _rs = mysql.getData(_sb.toString());
        while (_rs.next()) {
            _lstArticles.add(_rs.getString("name"));
        }
        _rs.close();
        return _lstArticles;
    }

    public void CleanRawData(MSClient mysql) throws SQLException {
        LogWriter.WriteTimeLog("START CLEAN RAW DATA ");
        long startTime = System.currentTimeMillis();

        mysql.ExecuteQuery("UPDATE " + MSClient.table_prefix + "_dbpediaRawData SET clean_name=SUBSTRING(name,30,LENGTH(name)-30)");

        mysql.ExecuteQuery("UPDATE " + MSClient.table_prefix + "_dbpediaRawData SET clean_datatype =SUBSTRING_INDEX(value, '\"',-1)  where value like '\"%'");

        mysql.ExecuteQuery("UPDATE " + MSClient.table_prefix + "_dbpediaRawData SET clean_value =SUBSTRING(value, 2,LENGTH(value)-2)  where value like '<%'");

        mysql.ExecuteQuery("UPDATE " + MSClient.table_prefix + "_dbpediaRawData SET clean_value= SUBSTRING(REPLACE(value,CONCAT('\"',clean_datatype),''),2) where value like '\"%' ");

        long estimatedTime = System.currentTimeMillis() - startTime;
        LogWriter.WriteTimeLog("END CLEAN RAW DATA ");
        LogWriter.WriteTimeLog("ELAPSED TIME " + estimatedTime);

    }

    public static void FillDbPediaTable(MSClient mysql, boolean InsertData) throws SQLException {


        StringBuilder _sb = new StringBuilder();

        if (InsertData) {
            mysql.ExecuteQuery("DELETE FROM " + MSClient.table_prefix + "_dbpediaData ");

            _sb.append("INSERT INTO " + MSClient.table_prefix + "_dbpediaData (article) ");
            _sb.append("SELECT DISTINCT clean_name ");
            _sb.append("FROM " + MSClient.table_prefix + "_dbpediaRawData ");
            mysql.ExecuteQuery(_sb.toString());
        }

        //REMOVE UNUSABLE VALUES

        _sb = new StringBuilder();
        _sb.append("DELETE " + MSClient.table_prefix + "_dbpediaData FROM " + MSClient.table_prefix + "_dbpediaData ");
        _sb.append("JOIN " + MSClient.table_prefix + "_dbpediaRawData RD ON " + MSClient.table_prefix + "_dbpediaData.article=RD.clean_name ");
        _sb.append("AND (");
        _sb.append("RD.property LIKE '%BirthDate%' ");
        _sb.append("OR RD.property LIKE '%dateOfBirth%' ");
        _sb.append("OR RD.property LIKE '%artist%' ");
        _sb.append("OR RD.property='<http://dbpedia.org/property/score>' ");
        _sb.append("OR RD.property='<http://dbpedia.org/property/referee>' ");
        _sb.append("OR RD.property='<http://dbpedia.org/property/coach>' ");
        _sb.append("OR RD.property='<http://dbpedia.org/property/league>' ");
        _sb.append("OR RD.value='<http://dbpedia.org/ontology/Season>' ");
        _sb.append("OR RD.property='<http://dbpedia.org/property/author>' ");
        _sb.append("OR RD.property='<http://dbpedia.org/property/edition>' ");
        _sb.append("OR RD.property='<http://dbpedia.org/property/collegeteam>' ");
        _sb.append("OR RD.property='<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>' AND RD.value='<http://dbpedia.org/ontology/Article>' ");
        _sb.append("OR RD.property='<http://dbpedia.org/property/champion>' ");
        _sb.append("OR RD.property='<http://dbpedia.org/property/rd1Team>' ");
        _sb.append("OR RD.property='<http://dbpedia.org/property/team>' ");
        _sb.append("");
        _sb.append("");
        _sb.append("");
        _sb.append(")");
        mysql.ExecuteQuery(_sb.toString());

        //UPDATE REMAINING VALUES
        _sb = new StringBuilder();
        _sb.append("UPDATE " + MSClient.table_prefix + "_dbpediaData DD ");
        _sb.append("INNER JOIN " + MSClient.table_prefix + "_dbpediaRawData RD ON RD.clean_name = DD.article ");
        _sb.append("AND RD.property='<http://dbpedia.org/property/name>' ");
        _sb.append("SET DD.name=RD.clean_value ");
        _sb.append("");
        mysql.ExecuteQuery(_sb.toString());

        _sb = new StringBuilder();
        _sb.append("UPDATE " + MSClient.table_prefix + "_dbpediaData DD ");
        _sb.append("INNER JOIN " + MSClient.table_prefix + "_dbpediaRawData RD ON RD.clean_name = DD.article ");
        _sb.append("AND RD.property='<http://dbpedia.org/property/website>' ");
        _sb.append("SET DD.website=RD.clean_value ");
        _sb.append("");
        mysql.ExecuteQuery(_sb.toString());

        _sb = new StringBuilder();
        _sb.append("UPDATE " + MSClient.table_prefix + "_dbpediaData DD ");
        _sb.append("INNER JOIN " + MSClient.table_prefix + "_dbpediaRawData RD ON RD.clean_name = DD.article ");
        _sb.append("AND RD.property='<http://xmlns.com/foaf/0.1/homepage>' ");
        _sb.append("SET DD.website=RD.clean_value ");
        _sb.append("");
        mysql.ExecuteQuery(_sb.toString());

        _sb = new StringBuilder();
        _sb.append("UPDATE " + MSClient.table_prefix + "_dbpediaData DD ");
        _sb.append("INNER JOIN " + MSClient.table_prefix + "_dbpediaRawData RD ON RD.clean_name = DD.article ");
        _sb.append("AND RD.property='<http://dbpedia.org/property/location>' ");
        _sb.append("SET DD.location=RD.clean_value ");
        _sb.append("");
        mysql.ExecuteQuery(_sb.toString());

        _sb = new StringBuilder();
        _sb.append("UPDATE " + MSClient.table_prefix + "_dbpediaData DD ");
        _sb.append("INNER JOIN " + MSClient.table_prefix + "_dbpediaRawData RD ON RD.clean_name = DD.article ");
        _sb.append("AND RD.property='<http://dbpedia.org/property/address>' ");
        _sb.append("SET DD.address=RD.clean_value ");
        _sb.append("");
        mysql.ExecuteQuery(_sb.toString());

        _sb = new StringBuilder();
        _sb.append("UPDATE " + MSClient.table_prefix + "_dbpediaData DD ");
        _sb.append("INNER JOIN " + MSClient.table_prefix + "_dbpediaRawData RD ON RD.clean_name = DD.article ");
        _sb.append("AND RD.property='<http://dbpedia.org/property/address>' ");
        _sb.append("SET DD.address=RD.clean_value ");
        _sb.append("");
        mysql.ExecuteQuery(_sb.toString());

        _sb = new StringBuilder();
        _sb.append("UPDATE " + MSClient.table_prefix + "_dbpediaData DD ");
        _sb.append("INNER JOIN " + MSClient.table_prefix + "_dbpediaRawData RD ON RD.clean_name = DD.article ");
        _sb.append("AND RD.property='<http://dbpedia.org/property/address>' ");
        _sb.append("SET DD.address=RD.clean_value ");
        _sb.append("");
        mysql.ExecuteQuery(_sb.toString());

        _sb = new StringBuilder();
        _sb.append("UPDATE " + MSClient.table_prefix + "_dbpediaData DD ");
        _sb.append("SET name_soundex=SOUNDEX(IFNULL(name,article))");
        _sb.append("");
        _sb.append("");
        _sb.append("");
        mysql.ExecuteQuery(_sb.toString());


        _sb = new StringBuilder();
        _sb.append("update " + MSClient.table_prefix + "_dbpediaData ");
        _sb.append("set category= (SELECT value FROM " + MSClient.table_prefix + "_dbpediarawdata FORCE INDEX (iRawCLNameProp) where property='<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>' and clean_name=article limit 1)");
        mysql.ExecuteQuery(_sb.toString());

        _sb = new StringBuilder();
        _sb.append("");
        _sb.append("");
        _sb.append("");
        _sb.append("");
        _sb.append("");


    }


}
