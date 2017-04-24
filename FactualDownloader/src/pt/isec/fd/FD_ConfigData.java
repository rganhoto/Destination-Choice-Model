package pt.isec.fd;

/**
 * Created by Rui on 18/02/2016.
 */
public class FD_ConfigData {

    private static FD_ConfigData _configdata =null;


    public String factual_key="";
    public String factual_secret="";

    public String factual_region="";
    public String factual_locality="";
    public String factual_country="";

    private FD_ConfigData()
    {

    }

    public static void Initialize(String key, String Secret,String Locality, String Region, String Country)
    {
        _configdata = new FD_ConfigData();
        _configdata.factual_key=key;
        _configdata.factual_secret=Secret;
        _configdata.factual_locality=Locality;
        _configdata.factual_region=Region;
        _configdata.factual_country=Country;
    }

    public static FD_ConfigData Get()
    {
        if(_configdata==null)
            throw new NullPointerException("FD_ConfigData is not initialized");
        return  _configdata;
    }
}
