package pt.isec;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Rui on 19/02/2016.
 */
public class ConfigReader {

    public static String mysql_address;
    public static int mysql_port;
    public static String mysql_user;
    public static String mysql_password;
    public static String mysql_database;
    public static String factual_key;
    public static String factual_secret;
    public static String factual_region;
    public static String factual_locality;
    public static String factual_country;
    public static String mysql_table_prefix;

    public static String cc_epsilon_type;
    public static String cc_zoom1_minpoints;
    public static String cc_zoom1_epsilon;
    public static String cc_zoom2_minpoints;
    public static String cc_zoom2_epsilon;
    public static String cc_zoom3_minpoints;
    public static String cc_zoom3_epsilon;

    public static String dbpedia_word;
    public static String dbpedia_path;
    public static String dbpedia_file1;
    public static String dbpedia_file2;
    public static String dbpedia_file3;
    public static String dbpedia_file4;
    public static String dbpedia_file5;
    public static String dbpedia_file6;
    public static String dbpedia_file7;
    public static String dbpedia_file8;
    public static String dbpedia_file9;
    public static String dbpedia_file10;
    public static String email;


    public static void ReadConfigFromFile() throws IOException {
        Properties prop = new Properties();
        FileInputStream is = new FileInputStream("config.properties");
        if (is != null) {
            prop.load(is);
        } else {
            throw new FileNotFoundException("property file config.properties not found");
        }
        mysql_address = prop.getProperty("mysql_address");


        mysql_port = Integer.parseInt(prop.getProperty("mysql_port"));
        mysql_user = prop.getProperty("mysql_user");
        mysql_password = prop.getProperty("mysql_password");
        mysql_database = prop.getProperty("mysql_database");
        factual_key = prop.getProperty("factual_key");
        factual_secret = prop.getProperty("factual_secret");
        factual_region = prop.getProperty("factual_region");
        factual_country = prop.getProperty("factual_country");
        factual_locality = prop.getProperty("factual_locality");
        mysql_table_prefix = prop.getProperty("mysql_table_prefix");

        cc_epsilon_type = prop.getProperty("cc_epsilon_type");
        cc_zoom1_epsilon = prop.getProperty("cc_zoom1_epsilon");
        cc_zoom1_minpoints = prop.getProperty("cc_zoom1_minpoints");
        cc_zoom2_epsilon = prop.getProperty("cc_zoom2_epsilon");
        cc_zoom2_minpoints = prop.getProperty("cc_zoom2_minpoints");
        cc_zoom3_epsilon = prop.getProperty("cc_zoom3_epsilon");
        cc_zoom3_minpoints = prop.getProperty("cc_zoom3_minpoints");
        dbpedia_word = prop.getProperty("dbpedia_word");
        dbpedia_path = prop.getProperty("dbpedia_path");
        dbpedia_file1 = prop.getProperty("dbpedia_file1");
        dbpedia_file2 = prop.getProperty("dbpedia_file2");
        dbpedia_file3 = prop.getProperty("dbpedia_file3");
        dbpedia_file4 = prop.getProperty("dbpedia_file4");
        dbpedia_file5 = prop.getProperty("dbpedia_file5");
        dbpedia_file6 = prop.getProperty("dbpedia_file6");
        dbpedia_file7 = prop.getProperty("dbpedia_file7");
        dbpedia_file8 = prop.getProperty("dbpedia_file8");
        dbpedia_file9 = prop.getProperty("dbpedia_file9");
        dbpedia_file10 = prop.getProperty("dbpedia_file10");
        email = prop.getProperty("email");
    }

}
