package pt.isec.fc.internal;

import com.sun.xml.internal.ws.api.model.wsdl.WSDLBoundOperation;
import processing.data.Sort;

import java.awt.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

/**
 * Created by Rui on 10/12/2016.
 */
public class eNeighborData {
    public String factual_id;
    public double Latitude = 0;
    public double Longitude = 0;


    public double LastDistance = -1;
    //public String factual_id_dist;
    public Integer PointCount = 0;

    ArrayList<Double> _Dists = new ArrayList<Double>();


    public double Distance(eNeighborData another) {
        return Math.sqrt(Math.pow(Math.abs(Latitude - another.Latitude), 2) + Math.pow(Math.abs(Longitude - another.Longitude), 2));
    }

    public void SetMinDistance2(double _dist, int NNeighbor) {
        if (_Dists.size() == NNeighbor) {
            if (LastDistance > _dist) {
                _Dists.remove(LastDistance);
                _Dists.add(_dist);
                LastDistance = Collections.max(_Dists);
            }
        } else {
            _Dists.add(_dist);
            if (LastDistance < _dist)
                LastDistance = _dist;
        }
    }

    public void SetMinDistance(eNeighborData another, int NNeighbor) {
        double _dist = Distance(another);
        if (_dist > 0) {
            this.SetMinDistance2(_dist, NNeighbor);
            another.SetMinDistance2(_dist, NNeighbor);
        }
        /*if(LastDistance == -1 || LastDistance >_dist)
        {
            this.factual_id_dist = another.factual_id;
            this.LastDistance = _dist;

            another.LastDistance = _dist;
            another.factual_id_dist = this.factual_id;
        }*/
    }

    public void IsInDistance(eNeighborData another, double epsilon) {
        double _dist = Distance(another);
        if (_dist <= epsilon) {
            another.PointCount++;
            this.PointCount++;
        }
    }
}
