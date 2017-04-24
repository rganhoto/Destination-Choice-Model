package pt.isec.fc;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Rui on 14/03/2016.
 */
public class FC_ExportShapeFile {


    ArrayList<Cluster> clusters = new ArrayList<Cluster>();
    ArrayList<RoundCornerHull> hulls = new ArrayList<RoundCornerHull>();




    class Cluster {
        ArrayList<Poi> pois = new ArrayList<Poi>();
        String id = "";
        PVector centroid_pos = new PVector();
        HashMap name_weights = new HashMap();

    }

    class Poi {
        String name = "";
        String source = "";
        PVector pos = new PVector();
        PVector latLon = new PVector();
        double mx = 0, my = 0;
        double lon = 0, lat = 0;
    }

    public class RubberHull {
        ArrayList<PVector> vertices;
        ArrayList<Edge> edges;
        ArrayList<PVector> staticPoints;

        // pushPoint properties
        float gravityForce = 0.5f; // lower number for better accuracy but slows the process
        float minLength = 50; // segments smaller this length will have no push force
        float springLengthFact = 0.9f; // spring length in function of length

        boolean simulate = true;
        boolean sizeOne = false;
    }

    class Edge {
        PVector a, b;
        PushPoint pushPoint;
        float length = 0;
        float minDifference = 5;

        // pushPoint properties
        float gravityForce, minLength, springLengthFact;

    }

    class PushPoint {
        PVector pos, force, iniPoint, gravityVec, resistanceVec;
        float length;
        float gravityForce, minLength, maxSrpingLength, springLengthFact;

    }

    public class RoundCornerHull {
        ArrayList<Segment> innerShape; //the real hull of set of points
        ArrayList<Segment> outerShape; //holder for all of segments of rounded corners hull
        ArrayList<Segment> outerParallel; //holder for only segments that are parallel to inner hull segments
        float shpWidth = 50; //the distance between inner hull segments and outer segments
        final private int ADAPTIVE = 1; //adaptive method
        final private int CONSTANT = 2; //static method -> not implemented


        boolean dsplSkltn = false; //debug

        ArrayList<Corner> corners;
        ArrayList<PVector> outerVertices;

    }

    class Segment {
        PVector a, b;
        float offset = 0;
        float startAngleSmooth = 1; // [0, 1]
        float absAngSmooth = 5; // [0, length()/2]

    }

    class Corner {
        ArrayList<Segment> inSegments;
        ArrayList<PVector> iPoints;
        static final int OPEN = 1;
        static final int CLOSED = 2;
        ArrayList<Segment> segments;

        int type = OPEN;

        float shpWidth = 50; //the distance between inner hull segments and outer segments
        float arcLengthStep = 5; //minimal lengh of arc of round corner
    }

    class PPath extends ArrayList<Segment> {
        float totalLength = 0;
        float normPosition = 0.0f;
        float absPosition = 0.0f;
        String id;
        boolean haveSpace = true;

    }

    class PVector
    {
        public float x;
        public float y;


    }
}
