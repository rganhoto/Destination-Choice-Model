package pt.isec.fd.internal;

import com.factual.driver.Factual;
import com.factual.driver.Query;
import com.factual.driver.ReadResponse;
import com.sun.corba.se.impl.orb.PrefixParserAction;
import com.sun.corba.se.pept.transport.ResponseWaitingRoom;
import com.sun.servicetag.SystemEnvironment;
import com.sun.xml.internal.bind.util.Which;
import org.omg.CORBA.Current;
import pt.isec.fd.FD_ConfigData;
import pt.isec.msh.LogWriter;
import pt.isec.msh.MSClient;
import pt.isec.msh.MS_StatementHlp;

import java.io.Console;
import java.security.InvalidParameterException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Rui on 20/02/2016.
 */
public class FactualDataAccessReader {
    static final int MAX_ROW_COUNT = 50;
    static final int MAX_ROW_COUNT_OFFSET = 500;
    static final String Dicionario = "0123456789abcdefghijklmnopqrstuvwxyz _-.,'&$#()=/";


    private MSClient _mysql;

    private Factual _factual;
    private FD_ConfigData _config;

    public FactualDataAccessReader(MSClient MySql) {
        this._mysql = MySql;
        _config = FD_ConfigData.Get();
        _factual = new Factual(_config.factual_key, _config.factual_secret);
    }

    public void GetAllData() throws SQLException {
        for (char i : Dicionario.toCharArray()) {
            if(String.valueOf(i).equals(" "))
                continue;
            for (char j : Dicionario.toCharArray()) {
                getPlaces(String.valueOf(i) + String.valueOf(j), 0);
            }
        }
    }

    public void ContinueData(String startValue) throws SQLException {
        if (startValue.length() < 2) {
            throw new InvalidParameterException("The start value must be 2 or more chars");
        }

        boolean _startRead = false;
        for (char i : Dicionario.toCharArray()) {
            if(String.valueOf(i).equals(" "))
                continue;
            for (char j : Dicionario.toCharArray()) {
                if (_startRead)
                    getPlaces(String.valueOf(i) + String.valueOf(j), 0);
                else if (startValue.startsWith(String.valueOf(i) + String.valueOf(j))) {
                    _startRead = true;
                    //SE ENCONTROU A STRING CORRETA
                    if (startValue.length() == 2)
                        getPlaces(String.valueOf(i) + String.valueOf(j), 0);
                    else
                        continueSubData(startValue, String.valueOf(i) + String.valueOf(j));
                }
            }
        }
    }

    private void continueSubData(String startValue, String CurrentValue) throws SQLException {
        boolean _startRead = false;
        for (char i : Dicionario.toCharArray()) {
            if (_startRead)
                getPlaces(CurrentValue + String.valueOf(i), 0);
            else if (startValue.startsWith(CurrentValue + String.valueOf(i))) {
                _startRead = true;
                //SE ENCONTROU A STRING CORRETA
                if (startValue.length() == CurrentValue.length() + 1)
                    getPlaces(CurrentValue + String.valueOf(i), 0);
                else
                    continueSubData(startValue, CurrentValue + String.valueOf(i));
            }
        }
    }

    private void getPlaces(String SearchString, int Offset) throws SQLException {
        Query q = new Query();

        q.includeRowCount();

        ArrayList<Query> _lst = new ArrayList<Query>();

        _lst.add(q.field("country").isEqual(_config.factual_country));

        if (!_config.factual_region.equals(""))
            _lst.add(q.field("region").isEqual(_config.factual_region));

        if (!_config.factual_locality.equals(""))
            _lst.add(q.field("locality").isEqual(_config.factual_locality));

        _lst.add(q.field("name").beginsWith(SearchString));

        Query[] queryArr = new Query[_lst.size()];
        queryArr = _lst.toArray(queryArr);

        q.and(queryArr);

        q.offset(Offset)
                .limit(MAX_ROW_COUNT)
                .only("name", "tel", "locality", "region", "latitude", "longitude", "category_ids", "category_labels", "postcode",
                        "factual_id", "address", "address_extended", "website", "hours", "neighborhood", "chain_id", "email", "chain_name")
                .sortAsc("name");

        ReadResponse _resp = null;
        try {
            _resp = _factual.fetch("places", q);
        } catch (Exception e) {
            //ESPERAR 15 MINS
            //TENTAR DE NOVO
            e.printStackTrace();
            LogWriter.WriteErrorLogs(e);
            try {
                System.out.println();
                System.out.println();
                System.out.println("Wait 15m");
                TimeUnit.MINUTES.sleep(15);
                _resp = _factual.fetch("places", q);
            } catch (Exception e2) {
                //ESPERAR 24h:5min
                //TENTAR DE NOVO

                e2.printStackTrace();
                LogWriter.WriteErrorLogs(e2);
                System.out.println();
                System.out.println();
                System.out.println("Wait 24h");
                System.out.println("SEARCH: " + SearchString);
                LogWriter.WriteSucessLog("WAIT 24 FOR " + SearchString);
                try {
                    TimeUnit.HOURS.sleep(24);
                    TimeUnit.MINUTES.sleep(5);
                    _resp = _factual.fetch("places", q);
                } catch (Exception e3) {
                    e3.printStackTrace();
                    LogWriter.WriteErrorLogs(e3);
                }
            }
        }
        treatResponse(SearchString, Offset, _resp);
    }

    private void treatResponse(String SearchString, int Offset, ReadResponse Response) throws SQLException {
        //CONTINUE SEARCH
        LogWriter.WriteSucessLog(SearchString + " GOT " + Response.getTotalRowCount() + " VALUES");

        if (Response.getTotalRowCount() >= MAX_ROW_COUNT_OFFSET) {
            for (char c : Dicionario.toCharArray()) {
                if(!SearchString.endsWith("  "))
                    getPlaces(SearchString + String.valueOf(c), 0);
            }
            return;
        }

        List<Map<String, Object>> lstMap = Response.getData();
        if (Response.getTotalRowCount() > 0) {
            System.out.println(".");
            insertIntoDatabase(lstMap);

            if (Response.getTotalRowCount() > Offset + MAX_ROW_COUNT) {
                Offset += MAX_ROW_COUNT;
                getPlaces(SearchString, Offset);
            }
        }
    }

    private void insertIntoDatabase(List<Map<String, Object>> lstMap) throws SQLException {
        _mysql.CloseConnectionIfOpen();
        _mysql.OpenConnection();
        StringBuilder sb = new StringBuilder();
        sb.append("REPLACE INTO " + MSClient.table_prefix + "_FactualData ");
        sb.append("(factual_id, name, tel, locality, region, latitude, longitude, category_ids, category_labels, postcode, address, address_extended, website, hours, neighborhood, email, chain_id, chain_name, name_soundex )");
        sb.append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SOUNDEX(?))");


        StringBuilder sbcat = new StringBuilder();
        sbcat.append("REPLACE INTO " + MSClient.table_prefix + "_FactualDataCategory ");
        sbcat.append("(factual_id, category_id) ");
        sbcat.append("VALUES (?,?) ");

        PreparedStatement stmt = null;
        PreparedStatement stmtcat = null;
        try {
            stmt = _mysql.PrepareStatement(sb.toString());
            stmtcat = _mysql.PrepareStatement(sbcat.toString());

            for (Map<String, Object> mp : lstMap) {
                try {

                    MS_StatementHlp.SetString(stmt, 1, mp.get("factual_id"));
                    MS_StatementHlp.SetString(stmt, 2, mp.get("name"));
                    MS_StatementHlp.SetString(stmt, 3, mp.get("tel"));
                    MS_StatementHlp.SetString(stmt, 4, mp.get("locality"));
                    MS_StatementHlp.SetString(stmt, 5, mp.get("region"));

                    MS_StatementHlp.SetDecimal(stmt, 6, mp.get("latitude"));
                    MS_StatementHlp.SetDecimal(stmt, 7, mp.get("longitude"));

                    MS_StatementHlp.SetString(stmt, 8, mp.get("category_ids"));
                    MS_StatementHlp.SetString(stmt, 9, mp.get("category_labels"));
                    MS_StatementHlp.SetString(stmt, 10, mp.get("postcode"));
                    MS_StatementHlp.SetString(stmt, 11, mp.get("address"));
                    MS_StatementHlp.SetString(stmt, 12, mp.get("address_extended"));
                    MS_StatementHlp.SetString(stmt, 13, mp.get("website"));
                    MS_StatementHlp.SetString(stmt, 14, mp.get("hours"));
                    MS_StatementHlp.SetString(stmt, 15, mp.get("neighborhood"));
                    MS_StatementHlp.SetString(stmt, 16, mp.get("email"));
                    MS_StatementHlp.SetString(stmt, 17, mp.get("chain_id"));
                    MS_StatementHlp.SetString(stmt, 18, mp.get("chain_name"));
                    MS_StatementHlp.SetString(stmt, 19, mp.get("name"));

                    stmt.executeUpdate();


                    if (mp.get("category_ids") != null) {

                        String categorias = mp.get("category_ids").toString().replace("[", "").replace("]", "");

                        for (String c : categorias.split(",")) {
                            MS_StatementHlp.SetString(stmtcat, 1, mp.get("factual_id"));
                            MS_StatementHlp.SetInt(stmtcat, 2, c);
                            stmtcat.executeUpdate();
                        }
                    }

                } catch (SQLException e) {
                    e.printStackTrace();
                    LogWriter.WriteErrorLogs(e);
                }
            }

            _mysql.FinishStatement();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            LogWriter.WriteErrorLogs(e);
        }
    }

    public void GetCategories() {
        Query q = new Query()
                .limit(500);
        q.only("category_id", "parents", "en", "pt", "abstract");

//obter categorias
        ReadResponse _resp = _factual.fetch("place-categories", q);
        List<Map<String, Object>> lstMap = _resp.getData();


        Dictionary<Integer, FactualCategory> _dicCategorias = new Hashtable<Integer, FactualCategory>();
        //preencher classe da categoria

        for (Map<String, Object> mp : lstMap) {
            FactualCategory cat = new FactualCategory();
            cat.category_id = Integer.valueOf(mp.get("category_id").toString());
            if (mp.get("parents") == null)
                cat.parents = 0;
            else
                cat.parents = Integer.valueOf(mp.get("parents").toString().replace("[", "").replace("]", ""));

            cat.en = mp.get("en").toString();
            cat.pt = mp.get("pt").toString();
            cat.Abstract = Boolean.valueOf(mp.get("abstract").toString());

            _dicCategorias.put(cat.category_id, cat);
        }


        Enumeration<FactualCategory> elems = _dicCategorias.elements();

        while (elems.hasMoreElements()) {
            FactualCategory _cat = elems.nextElement();

            _cat.base_category_id = _cat.category_id;

            if (_cat.parents > 0 && _cat.Abstract == false) {
                FactualCategory _catp = _dicCategorias.get(_cat.parents);
                while (_catp.parents > 0 && _catp.Abstract == false) {
                    _cat.base_category_id = _catp.category_id;
                    _catp = _dicCategorias.get(_catp.parents);
                }
            }
        }


        //registar a categoria na base de dados
        elems = _dicCategorias.elements();

        StringBuilder sb = new StringBuilder();
        sb.append("REPLACE INTO " + MSClient.table_prefix + "_FactualCategory ");
        sb.append("(category_id, parents, base_category_id, en, pt)");
        sb.append("VALUES (?, ?, ?, ?, ?)");
        PreparedStatement stmt = null;
        try {
            stmt = _mysql.PrepareStatement(sb.toString());

            while (elems.hasMoreElements()) {
                FactualCategory _cat = elems.nextElement();

                stmt.setInt(1, _cat.category_id);
                stmt.setInt(2, _cat.parents);
                stmt.setInt(3, _cat.base_category_id);
                stmt.setString(4, _cat.en);
                stmt.setString(5, _cat.pt);

                stmt.executeUpdate();

            }

            _mysql.FinishStatement();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            LogWriter.WriteErrorLogs(e);
        }
    }


    public void GetFactualCrossWalk(String StartID) throws SQLException {
        //LER FACTUAL ID's

        ArrayList<String> _lstIDs = ReadFactualIDs(StartID);

        ArrayList<String> _lstIDLimited = new ArrayList<String>();


/*        if (StartID.length() > 10) {
            while (_lstIDs.size() > 0) {
                if (_lstIDs.get(0).toUpperCase() == StartID.toUpperCase()) {
                    break;
                }
                _lstIDs.remove(0);
            }
        } */
        for (String factual_id : _lstIDs) {
            _lstIDLimited.add(factual_id);

            if (_lstIDLimited.size() >= 10) {
                getCrossWalk(_lstIDLimited);
                _lstIDLimited = new ArrayList<String>();
            }
        }

       /* _lstIDLimited.add("54c962b8-1730-4779-b118-bafc1720258c");
        _lstIDLimited.add("024d6c3e-93de-41e3-b7c0-ee8c8f8e6b1a");
        _lstIDLimited.add("0d2f9d05-f984-4224-b808-055abd90fa0d");
*/
        if (_lstIDLimited.size() > 0)
            getCrossWalk(_lstIDLimited);


        //CHAMAR CROSSWALK COM 20 ID's de Cada Vez
        //Tratar Crosswalk

    }

    private ArrayList<String> ReadFactualIDs(String StartID) throws SQLException {
        ArrayList<String> _lstFactualIDs = new ArrayList<String>();

        String strSQL = "SELECT distinct factual_id FROM " + MSClient.table_prefix + "_FactualData "  ;

        if(StartID.length()>5)
            strSQL += " WHERE factual_id >= '" + StartID.trim() + "' ";

        strSQL+= " ORDER BY factual_id";

        ResultSet rs = _mysql.getData(strSQL);

        while (rs.next()) {
            String factual_id = rs.getString("factual_id");
            _lstFactualIDs.add(factual_id);
        }
        rs.close();

        return _lstFactualIDs;
    }

    private void getCrossWalk(ArrayList<String> lstString) {
        Query q = new Query();
        q.includeRowCount();


        FD_ConfigData _conf = FD_ConfigData.Get();

        String[] queryArr = new String[lstString.size()];
        queryArr = lstString.toArray(queryArr);

        q.and(q.field("factual_id").in((Object[]) queryArr), q.field("namespace").isEqual("wikipedia"));
        q.limit(MAX_ROW_COUNT)
                .only("factual_id", "url", "namespace");


        LogWriter.WriteSucessLog("READING CROSSWALK: " + lstString.get(lstString.size() - 1));

        ReadResponse _resp = null;
        try {
            _resp = _factual.fetch("crosswalk-" + _conf.factual_country.toLowerCase(), q);
        } catch (Exception e) {
            //ESPERAR 15 MINS
            //TENTAR DE NOVO
            e.printStackTrace();
            LogWriter.WriteErrorLogs(e);
            try {
                System.out.println();
                System.out.println();
                System.out.println("Wait 15m");
                TimeUnit.MINUTES.sleep(15);
                _resp = _factual.fetch("crosswalk-" + _conf.factual_country.toLowerCase(), q);
            } catch (Exception e2) {
                //ESPERAR 24h:5min
                //TENTAR DE NOVO

                e2.printStackTrace();
                LogWriter.WriteErrorLogs(e2);
                System.out.println();
                System.out.println();
                System.out.println("Wait 24h");
                try {
                    TimeUnit.HOURS.sleep(24);
                    TimeUnit.MINUTES.sleep(5);
                    _resp = _factual.fetch("crosswalk-" + _conf.factual_country.toLowerCase(), q);
                } catch (Exception e3) {
                    e3.printStackTrace();
                    LogWriter.WriteErrorLogs(e3);
                }
            }
        }
        treatCrosswalkResponse(_resp);
    }

    private void treatCrosswalkResponse(ReadResponse resp) {
        InsertCrosswalkIntoDataBase(resp.getData());
    }

    private void InsertCrosswalkIntoDataBase(List<Map<String, Object>> lstMap) {
        StringBuilder sb = new StringBuilder();
        sb.append("REPLACE INTO " + MSClient.table_prefix + "_FactualCrosswalk ");
        sb.append("(factual_id, url, namespace )");
        sb.append("VALUES (?, ?, ?)");
        PreparedStatement stmt = null;
        try {
            _mysql.OpenConnection();
            stmt = _mysql.PrepareStatement(sb.toString());
            for (Map<String, Object> mp : lstMap) {
                try {

                    MS_StatementHlp.SetString(stmt, 1, mp.get("factual_id"));
                    MS_StatementHlp.SetString(stmt, 2, mp.get("url"));
                    MS_StatementHlp.SetString(stmt, 3, mp.get("namespace"));

                    stmt.executeUpdate();

                } catch (SQLException e) {
                    e.printStackTrace();
                    LogWriter.WriteErrorLogs(e);
                }
            }
            _mysql.FinishStatement();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            LogWriter.WriteErrorLogs(e);
        }
    }

    private class FactualCategory {
        public int category_id;
        public int parents;
        public String en;
        public String pt;
        public boolean Abstract;
        public int base_category_id;
    }


}
