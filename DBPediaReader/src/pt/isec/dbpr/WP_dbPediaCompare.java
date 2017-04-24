package pt.isec.dbpr;

import com.sun.corba.se.spi.orbutil.fsm.Guard;
import com.sun.xml.internal.ws.util.StringUtils;
import com.wcohen.ss.*;
import pt.isec.dbpr.Internal.eComparsion;
import pt.isec.msh.MSClient;
import pt.isec.msh.MS_StatementHlp;
import sun.misc.Compare;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Rui on 24/05/2016.
 */
public class WP_dbPediaCompare {


    public static void CompareFactualVSdbPedia(MSClient mysql) throws SQLException {
        //INSERT VALUES
        WP_dbPediaCompare _cmp = new WP_dbPediaCompare();
        _cmp.CreateTable(mysql);
        _cmp.InitiateWork(mysql);

        //START READING VALUES
        // _cmp.Compare100Rows(mysql);

    }

   /* public static void ComparePreInsertedData(MSClient mysql) throws SQLException {
        WP_dbPediaCompare _cmp = new WP_dbPediaCompare();
        boolean _hasRows = true;
        int Count = 0;

        do {
            ++Count;
            _hasRows = _cmp.Compare100Rows(mysql);
            System.out.print(".");
            if (Count > 100) {
                System.out.println();
                Count = 0;
            }
        } while (_hasRows);
    }*/

    public void CreateTable(MSClient mysql) throws SQLException {
        StringBuilder _sb = null;
        if (!mysql.TableExists(MSClient.table_prefix + "_Comparsion")) {
            _sb = new StringBuilder();
            _sb.append("CREATE TABLE " + MSClient.table_prefix + "_Comparsion ( ");
            _sb.append("dbpedia_article VARCHAR(200) NOT NULL, ");
            _sb.append("factual_id VARCHAR(50) NOT NULL, ");
            _sb.append("name_jacard DECIMAL(15,10) NULL, ");
            _sb.append("name_jaro DECIMAL(15,10) NULL, ");
            _sb.append("name_levenstein DECIMAL(15,10) NULL, ");
            _sb.append("name_mongeelkan DECIMAL(15,10) NULL, ");
            _sb.append("name_smithwaterman DECIMAL(15,10) NULL, ");
            _sb.append("name_unsmoothedjs DECIMAL(15,10) NULL, ");
            _sb.append("website_jacard DECIMAL(15,10) NULL, ");
            _sb.append("website_jaro DECIMAL(15,10) NULL, ");
            _sb.append("website_levenstein DECIMAL(15,10) NULL, ");
            _sb.append("website_mongeelkan DECIMAL(15,10) NULL, ");
            _sb.append("website_smithwaterman DECIMAL(15,10) NULL, ");
            _sb.append("website_unsmoothedjs DECIMAL(15,10) NULL, ");
            _sb.append("distance DECIMAL(15,10) NULL, ");
            _sb.append("processed BIT NOT NULL, ");
            _sb.append("PRIMARY KEY (dbpedia_article, factual_id)");
            _sb.append(")");

            mysql.ExecuteQuery(_sb.toString());
        }
    }


    public void InitiateWork(MSClient mysql) throws SQLException {


        //GET FACTUAL ID's

        String _lastFactualID = "";

        //GET CURRENT FACTUAL ID

        String _startupID = mysql.getString("SELECT MAX(factual_id) FROM " + MSClient.table_prefix + "_Comparsion ");

        boolean ReadMore = true;

        do {

            StringBuilder _sb = new StringBuilder();

            //OBTER ID DO FACTUAL
            _sb.append("SELECT MIN(factual_id) factual_id ");
            _sb.append("FROM " + MSClient.table_prefix + "_FactualData F ");
            if (!_lastFactualID.equals(""))
                _sb.append("WHERE factual_id > '" + _lastFactualID + "'");
            else if (_startupID != null && !_startupID.equals(""))
                _sb.append("WHERE factual_id = '" + _startupID + "'");

            ResultSet _rs = mysql.getData(_sb.toString());

            ReadMore = false;
            if (_rs.next()) {
                _lastFactualID = _rs.getString(1);
                System.out.println(_lastFactualID);
                _rs.close();
                ReadMore = CompareRows(mysql, _lastFactualID);
            } else
                _rs.close();


/*            _sb.append("SELECT factual_id ");
            _sb.append("FROM " + MSClient.table_prefix + "_FactualData F ");

            if (!_lastFactualID.equals(""))
                _sb.append("WHERE factual_id > '" + _lastFactualID + "'");

            _sb.append("ORDER BY factual_id LIMIT 1");

            ResultSet rs = mysql.getData(_sb.toString());
*/
        } while (ReadMore);
    }


    public boolean CompareRows(MSClient mysql, String FactualID) throws SQLException {

        boolean hasRows = false;

        StringBuilder _sb = new StringBuilder();
        _sb.append("SELECT D.article 'dbpedia_article', F.factual_id, F.name 'factual_name', D.name 'dbpedia_name', ");
        _sb.append("F.website 'factual_website', D.website 'dbpedia_website', ");
        _sb.append("F.latitude 'factual_latitude', F.longitude 'factual_longitude', ");
        _sb.append("D.latitude 'dbpedia_latitude', D.longitude 'dbpedia_longitude' ");
        _sb.append("FROM " + MSClient.table_prefix + "_dbpediaData D ");
        _sb.append("INNER JOIN " + MSClient.table_prefix + "_FactualData F ON F.factual_id='" + FactualID + "'  ");


      /*  _sb.append("SELECT C.dbpedia_article, C.factual_id, F.name 'factual_name', D.name 'dbpedia_name', ");
        _sb.append("F.website 'factual_website', D.website 'dbpedia_website', ");
        _sb.append("F.latitude 'factual_latitude', F.longitude 'factual_longitude', ");
        _sb.append("D.latitude 'dbpedia_latitude', D.longitude 'dbpedia_longitude' ");
        _sb.append("FROM " + MSClient.table_prefix + "_Comparsion C ");
        _sb.append("INNER JOIN " + MSClient.table_prefix + "_dbpediaData D ON D.article=C.dbpedia_article ");
        _sb.append("INNER JOIN " + MSClient.table_prefix + "_FactualData F ON F.factual_id=C.factual_id   ");
        _sb.append("WHERE C.processed=0 ");
        _sb.append("LIMIT 100 ");

*/
        ResultSet rs = mysql.getData(_sb.toString());
        while (rs.next()) {
            hasRows = true;
            String _dbpediaName = rs.getString("dbpedia_name");
            String _factualName = rs.getString("factual_name");

            if (_dbpediaName == null || _dbpediaName.equals(""))
                _dbpediaName = rs.getString("dbpedia_article");

            if (IsValidComparsion(_factualName, rs.getString("factual_website"), _dbpediaName, rs.getString("dbpedia_website"))) {
                eComparsion _compareName = Compare(_factualName, _dbpediaName);

                eComparsion _compareWebsite = Compare(rs.getString("factual_website"), rs.getString("dbpedia_website"));

                double dist = Distance(rs.getString("factual_latitude"), rs.getString("factual_longitude"),
                        rs.getString("dbpedia_latitude"), rs.getString("dbpedia_longitude"));


                //UpdateRow(mysql, rs.getString("factual_id"), rs.getString("dbpedia_article"), _compareName, _compareWebsite, dist);
                InsertRow(mysql, rs.getString("factual_id"), rs.getString("dbpedia_article"), _compareName, _compareWebsite, dist);
            }


        }
        rs.close();
        return hasRows;
    }

    private static eComparsion Compare(String FactualCompareString, String dbPediaCompareString) {

        if (FactualCompareString == null || FactualCompareString.trim().equals(""))
            return null;
        if (dbPediaCompareString == null || dbPediaCompareString.trim().equals(""))
            return null;

        eComparsion comp = new eComparsion();
        comp.factualCompareString = FactualCompareString;
        comp.wikipediaCompareString = dbPediaCompareString;

        Jaccard jc = new Jaccard();
        comp.jacard = jc.score(comp.factualCompareString, comp.wikipediaCompareString);

        Jaro jr = new Jaro();
        comp.jaro = jr.score(comp.factualCompareString, comp.wikipediaCompareString);

        Levenstein ls = new Levenstein();
        comp.levenstein = ls.score(comp.factualCompareString, comp.wikipediaCompareString);

        MongeElkan me = new MongeElkan();
        comp.mongeelkan = me.score(comp.factualCompareString, comp.wikipediaCompareString);

        SmithWaterman sm = new SmithWaterman();
        comp.smithwaterman = sm.score(comp.factualCompareString, comp.wikipediaCompareString);

        UnsmoothedJS js = new UnsmoothedJS();
        comp.unsmoothedjs = js.score(comp.factualCompareString, comp.wikipediaCompareString);

        return comp;
    }

    public double Distance(String Latitude1, String Longitude1, String Latitude2, String Longitude2) {
        try {
            if (Latitude1 == null || Latitude2 == null || Longitude1 == null || Longitude2 == null)
                return 9999;

            double dblLat1 = Double.parseDouble(Latitude1);
            double dblLat2 = Double.parseDouble(Latitude2);
            double dblLon1 = Double.parseDouble(Longitude1);
            double dblLon2 = Double.parseDouble(Longitude2);

            return Math.sqrt(
                    Math.pow(dblLat1 - dblLat2, 2) +
                            Math.pow(dblLon1 - dblLon2, 2));

        } catch (Exception ex) {
            //ex.printStackTrace();
            return 9999;
        }
    }

    public void UpdateRow(MSClient mysql, String factual_id, String dbpedia_article, eComparsion CompareName, eComparsion CompareWebsite, double Distance) throws SQLException {

        StringBuilder _sb = new StringBuilder();
        _sb.append("UPDATE " + MSClient.table_prefix + "_Comparsion ");
        _sb.append("SET processed=1 ");

        if (Distance != 9999)
            _sb.append(", distance=? ");

        if (CompareName != null)
            _sb.append(", name_jacard=?, name_jaro=?, name_levenstein=?, name_mongeelkan=?, name_smithwaterman=?, name_unsmoothedjs=? ");

        if (CompareWebsite != null)
            _sb.append(", website_jacard=?, website_jaro=?, website_levenstein=?, website_mongeelkan=?, website_smithwaterman=?, website_unsmoothedjs=? ");


        _sb.append("WHERE factual_id=? AND dbpedia_article=?");

        PreparedStatement _stmt = mysql.PrepareStatement(_sb.toString());

        int _index = 0;
        if (Distance != 9999)
            MS_StatementHlp.SetDecimal(_stmt, ++_index, Distance);

        if (CompareName != null) {
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareName.jacard);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareName.jaro);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareName.levenstein);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareName.mongeelkan);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareName.smithwaterman);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareName.unsmoothedjs);
        }

        if (CompareWebsite != null) {
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareWebsite.jacard);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareWebsite.jaro);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareWebsite.levenstein);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareWebsite.mongeelkan);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareWebsite.smithwaterman);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareWebsite.unsmoothedjs);
        }

        MS_StatementHlp.SetString(_stmt, ++_index, factual_id);
        MS_StatementHlp.SetString(_stmt, ++_index, dbpedia_article);

        _stmt.executeUpdate();
        mysql.FinishStatement();
        _stmt.close();
    }


    public void InsertRow(MSClient mysql, String factual_id, String dbpedia_article, eComparsion CompareName, eComparsion CompareWebsite, double Distance) throws SQLException {

        StringBuilder _sbValues = new StringBuilder();
        StringBuilder _sbFields = new StringBuilder();


        if (Distance != 9999) {
            _sbFields.append(", distance");
            _sbValues.append(", ?");
        }

        if (CompareName != null) {
            _sbFields.append(", name_jacard, name_jaro, name_levenstein, name_mongeelkan, name_smithwaterman, name_unsmoothedjs");
            _sbValues.append(", ?, ?, ?, ?, ?, ?");
        }
//            _sb.append(", name_jacard=?, name_jaro=?, name_levenstein=?, name_mongeelkan=?, name_smithwaterman=?, name_unsmoothedjs=? ");

        if (CompareWebsite != null) {
            _sbFields.append(", website_jacard, website_jaro, website_levenstein, website_mongeelkan, website_smithwaterman, website_unsmoothedjs");
            _sbValues.append(", ?, ?, ?, ?, ?, ?");
        }
        //_sb.append(", website_jacard=?, website_jaro=?, website_levenstein=?, website_mongeelkan=?, website_smithwaterman=?, website_unsmoothedjs=? ");

        StringBuilder _sb = new StringBuilder();
        _sb.append("REPLACE INTO " + MSClient.table_prefix + "_Comparsion(factual_id, dbpedia_article, Processed ");
        _sb.append(_sbFields.toString());
        _sb.append(") VALUES (?,?,1 ");
        _sb.append(_sbValues.toString());
        _sb.append(")");


        PreparedStatement _stmt = mysql.PrepareStatement(_sb.toString());

        int _index = 0;

        MS_StatementHlp.SetString(_stmt, ++_index, factual_id);
        MS_StatementHlp.SetString(_stmt, ++_index, dbpedia_article);

        if (Distance != 9999)
            MS_StatementHlp.SetDecimal(_stmt, ++_index, Distance);

        if (CompareName != null) {
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareName.jacard);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareName.jaro);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareName.levenstein);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareName.mongeelkan);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareName.smithwaterman);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareName.unsmoothedjs);
        }

        if (CompareWebsite != null) {
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareWebsite.jacard);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareWebsite.jaro);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareWebsite.levenstein);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareWebsite.mongeelkan);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareWebsite.smithwaterman);
            MS_StatementHlp.SetDecimal(_stmt, ++_index, CompareWebsite.unsmoothedjs);
        }


        _stmt.executeUpdate();
        mysql.FinishStatement();
        _stmt.close();
    }


    private boolean IsValidComparsion(String factualName, String factualWS, String dbPediaName, String dbPediaWS) {

        double _jacName = 9999;

        if (factualName == null || factualName.trim().equals(""))
            _jacName = 0;
        if (dbPediaName == null || dbPediaName.trim().equals(""))
            _jacName = 0;

        double _jacWS = 9999;
        if (factualWS == null || factualWS.trim().equals(""))
            _jacWS = 0;
        if (dbPediaWS == null || dbPediaWS.trim().equals(""))
            _jacWS = 0;

        if (_jacName == 0 && _jacWS == 0)
            return false;

        if (_jacName > 0) {
            Jaccard jc = new Jaccard();
            _jacName = jc.score(factualName, dbPediaName);
        }

        if (_jacWS > 0) {
            Jaccard jc = new Jaccard();
            _jacWS = jc.score(factualWS, dbPediaWS);
        }

        if (_jacName > 0 || _jacWS > 0.4)
            return true;

        return false;
    }


    public static void CompareWebSite_TEST(MSClient mysql) throws Exception {
        //GET ROWS
        StringBuilder _sb = new StringBuilder();
        _sb.append("select CR.dbpedia_article, CR.factual_id, db.website 'db_website', fd.website 'fd_website' ");
        _sb.append("from ny_comparsion_result CR ");
        _sb.append("INNER JOIN ny_dbpediadata db ON db.article = CR.dbpedia_article AND db.website is not null ");
        _sb.append("INNER JOIN ny_factualdata fd on fd.factual_id = CR.factual_id AND fd.website IS NOT NULL ");

        ResultSet rs = mysql.getData(_sb.toString());

        _sb = new StringBuilder();
        _sb.append("UPDATE ny_comparsion_result SET website_jacard=?, website_jaro=?, website_levenstein=?, website_mongeelkan=?, website_smithwaterman=?, website_unsmoothedjs=? ");
        _sb.append("where factual_id=? AND dbpedia_article=?");

        while (rs.next()) {
            //COMPARAR WEBSITES
            String _article = rs.getString("dbpedia_article");
            String _factualID = rs.getString("factual_id");
            String _db_website = NormalizeWebsite(rs.getString("db_website"));
            String _fd_website = NormalizeWebsite(rs.getString("fd_website"));


            eComparsion _comp = Compare(_db_website, _fd_website);
            if (_comp != null) {

                PreparedStatement stmt = mysql.PrepareStatement(_sb.toString());
                MS_StatementHlp.SetDecimal(stmt, 1, _comp.jacard);
                MS_StatementHlp.SetDecimal(stmt, 2, _comp.jaro);
                MS_StatementHlp.SetDecimal(stmt, 3, _comp.levenstein);
                MS_StatementHlp.SetDecimal(stmt, 4, _comp.mongeelkan);
                MS_StatementHlp.SetDecimal(stmt, 5, _comp.smithwaterman);
                MS_StatementHlp.SetDecimal(stmt, 6, _comp.unsmoothedjs);
                MS_StatementHlp.SetString(stmt, 7, _factualID);
                MS_StatementHlp.SetString(stmt, 8, _article);

                stmt.executeUpdate();
                mysql.FinishStatement();
                stmt.close();
            }
        }
        rs.close();
    }

    private static String NormalizeWebsite(String website) {
        if (website == null)
            return "";
        if (website.equals(""))
            return "";

        String _newWebsite = new String(website);

        if (website.startsWith("http://"))
            _newWebsite = website.replace("http://", "");
        if (website.startsWith("https://"))
            _newWebsite = website.replace("https://", "");


        String _LeftWebsite = _newWebsite;
        String _RightWebsite = "";

        if (_newWebsite.contains("/")) {
//SEPARAR COISAS À ESQUEDA DA BARRA E À DIREITA DA BARRA

            int _iBarra = _newWebsite.indexOf("/");
            _LeftWebsite = _newWebsite.substring(0, _iBarra);
            _RightWebsite = _newWebsite.substring(_iBarra);
        }


        int count = _LeftWebsite.length() - _LeftWebsite.replace(".", "").length();
        if (count >= 2) {
            //RETIRAR CENAS À ESQUERDA DO 1º PONTO
            int _iPonto = _LeftWebsite.indexOf(".");
            _LeftWebsite = _LeftWebsite.substring(_iPonto+1);
        }

        if(_RightWebsite.endsWith("/"))
            _RightWebsite = _RightWebsite.substring(0,_RightWebsite.length()-1);

        return _LeftWebsite + _RightWebsite;
    }


}
