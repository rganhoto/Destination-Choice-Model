package pt.isec.fc;

/**
 * Created by Rui on 20/02/2016.
 */
public class FC_ConfigData {

    private static FC_ConfigData _configdata = null;

    public String cc_epsilon_type = "";
    public int cc_zoom1_minpoints = 0;
    public float cc_zoom1_epsilon = 0;
    public int cc_zoom2_minpoints = 0;
    public float cc_zoom2_epsilon = 0;
    public int cc_zoom3_minpoints = 0;
    public float cc_zoom3_epsilon = 0;

    private FC_ConfigData() {

    }

    public static void Initialize(String EpsilonType,
                                  int Zoom1_minpoints, float Zoom1_epsilon,
                                  int Zoom2_minpoints, float Zoom2_epsilon,
                                  int Zoom3_minpoints, float Zoom3_epsilon) {
        _configdata = new FC_ConfigData();
        _configdata.cc_epsilon_type = EpsilonType;

        _configdata.cc_zoom1_minpoints = Zoom1_minpoints;
        _configdata.cc_zoom2_minpoints = Zoom2_minpoints;
        _configdata.cc_zoom3_minpoints = Zoom3_minpoints;

        if (EpsilonType.toUpperCase().equals("KILOMETER")) {
            _configdata.cc_zoom1_epsilon = Zoom1_epsilon/111.03f;
            _configdata.cc_zoom2_epsilon = Zoom2_epsilon/111.03f;
            _configdata.cc_zoom3_epsilon = Zoom3_epsilon/111.03f;
        } else if (EpsilonType.toUpperCase().equals("MILE")) {
            _configdata.cc_zoom1_epsilon = Zoom1_epsilon/68.99f;
            _configdata.cc_zoom2_epsilon = Zoom2_epsilon/68.99f;
            _configdata.cc_zoom3_epsilon = Zoom3_epsilon/68.99f;
        } else {
            _configdata.cc_zoom1_epsilon = Zoom1_epsilon;
            _configdata.cc_zoom2_epsilon = Zoom2_epsilon;
            _configdata.cc_zoom3_epsilon = Zoom3_epsilon;
        }
    }

    //http://www.longitudestore.com/how-big-is-one-gps-degree.html

    //SE FOR KM
    //1 GRAU = 111.03 KM
    //SE FOR MILHAS
    //1 GRAU = 68.99 MILHAS;


    public static FC_ConfigData Get() {
        if (_configdata == null)
            throw new NullPointerException("pt.isec.fc.FC_ConfigData is not initialized");
        return _configdata;
    }

    public static void RunRubberCLusters2(String[] strData )
    {
        RubberClusters.Run(strData);
    }

    public static void RunRubberCLusters()
    {

        String[] strData = new String[4];
        strData[0]="NY_cat_cluster_Z1.json";
        strData[1]="40.715755";
        strData[2]="-73.95467";
        strData[3]="10";

        RubberClusters.Run(strData);
/**/
/*
        strData[0]="SG_cat_cluster_Z2.json";
        strData[1]="1.3360327";
        strData[2]="103.835205";
        strData[3]="13";

        RubberClusters.Run(strData);
/**/
        /*strData[0]="PT_cat_cluster_Z3.json";
        strData[1]="39.063866";
        strData[2]="-9.330733";
        strData[3]="16";

        RubberClusters.Run(strData);
    /**/
    }
}
