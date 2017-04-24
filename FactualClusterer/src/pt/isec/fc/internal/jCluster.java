package pt.isec.fc.internal;

import java.util.ArrayList;

/**
 * Created by Rui on 20/02/2016.
 */
public class jCluster {

    private String id;
    private double lat;
    private double lon;

   public ArrayList<jName> name_weights = new ArrayList<jName>();
   public ArrayList<jPoi> pois = new ArrayList<jPoi>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public ArrayList<jName> getName_weights() {
        return name_weights;
    }

    public void setName_weights(ArrayList<jName> name_weights) {
        this.name_weights = name_weights;
    }

    public ArrayList<jPoi> getPois() {
        return pois;
    }

    public void setPois(ArrayList<jPoi> pois) {
        this.pois = pois;
    }




}
