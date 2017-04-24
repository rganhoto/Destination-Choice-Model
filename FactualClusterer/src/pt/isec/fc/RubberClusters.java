package pt.isec.fc;
/**
 * Created by zhekapol on 26/03/14.
 */


import megamu.mesh.Hull;
import megamu.mesh.MPolygon;
import processing.core.PApplet;
import processing.core.PConstants;
import processing.core.PShape;
import processing.core.PVector;
import processing.data.JSONArray;
import processing.data.JSONObject;
import zUtils.ZGeo;
import zUtils.ZGeometry;
import ztools.map.ZMap;
import ztools.map.geo.ZLocation;

import java.io.File;
import java.sql.Array;
import java.util.*;


public class RubberClusters extends PApplet {

    //final int NUMBER = 10000;
    final int NUMBER = 5000;

    public float[][] points = new float[50][2];
    public RubberHull rhull;
    public RoundCornerHull hull;
    public boolean debug = true;

    public ArrayList<Cluster> clusters;
    public Cluster oneCluster;

    ZGeo geoHelper;
    private String[] proj4Params = new String[]{"+proj=merc", "+a=6378137", "+b=6378137", "+lat_ts=0.0", "+lon_0=0.0", "+x_0=0.0", "+y_0=0", "+k=1.0", "+units=m", "+nadgrids=@null", "+wktext", "+no_defs"};

    public ZMap map;

    public ArrayList<RoundCornerHull> hulls;
    public ArrayList<RubberHull> rubHulls;

    public HashMap<Integer, RoundCornerHull> dichulls;
    public HashMap<Integer, RubberHull> dicrubHulls;

    String data_home = "/Applications/MAMP/htdocs/crowds_v2/data/clustersZoom/";
    String data_path = data_home + "parsedClusters/RW_epsion0.003Min6_centroids_withwords.json";
    String out_path = data_home + "geometries/new/RW_epsion0.003Min6_geoms.json";

    public void setup() {
        PApplet.println(args);

        size(1700, 1100);
        frameRate(50);
        // noLoop();
        smooth();
        colorMode(PConstants.HSB, 360, 100, 100, 100);

        map = new ZMap(this, "epsg:3785");

        try {
            File fin = new File(args[0]);
            String name = fin.getName();
            //data_home = fin.getParent();

            data_home = System.getProperty("user.dir");
            if (fin.getParent() != null)
                data_home += "\\" + fin.getParent();

            data_path = name;
            PApplet.println(data_path);
            out_path = "shape_" + name;

            // parametros
            float lat = PApplet.parseFloat(args[1]); // latitude
            float lon = PApplet.parseFloat(args[2]); // longitude
            int zoomLevel = PApplet.parseInt(args[3]); // nivel de zoom (10 geral: 16 zoom in)

            ZLocation loc = map.getTransformation().transform(new ZLocation(lon, lat));
            map.setGeoCenter((float) loc.x, (float) loc.y);
            map.setZoomCenter(width / 2 - 150, height / 2);

            float scl = PApplet.map(zoomLevel, 10, 16, 0.2f, 0.06f);
            map.setScale(scl); //default 0.2f / cluster set 1 = 0.06 /
            map.update();

        } catch (Exception e) {
            PApplet.println("Not enough arguments (Latitude Longitude ZoomLevel)");
            exit();
            return;
        }

        //map.setGeoCenter(-7918557.6427264055f, 5217297.9321117f);


        //geoHelper = new ZGeo(new PVector(-71.542f, 42.061f), new PVector(-70.605f, 42.641f), width, height, proj4Params);
        clusters = new ArrayList<Cluster>();
        loadData();

        rubHulls = new ArrayList<RubberHull>();
        hulls = new ArrayList<RoundCornerHull>();
        dichulls = new HashMap<Integer, RoundCornerHull>();
        dicrubHulls = new HashMap<Integer, RubberHull>();
        PApplet.println("FimLoadData");

        int c = 0;
        ArrayList<Thread> threads = new ArrayList<Thread>();

        for (Cluster clus : clusters) {
            c++;

            final Cluster cls = clus;
            final int Clus = c;

            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {

                    RubberHull rrhull = new RubberHull(cls.getUniquePositions(), 0.5f, 10f, 2f); //0.1, 10, 2
                    for (int i = 0; i < NUMBER; i++) {

                        //PApplet.print(".");
                        rrhull.update();
                    }
                    RoundCornerHull rchull = new RoundCornerHull(rrhull.getVertices(), 10f);
                    dicrubHulls.put(Clus, rrhull);
                    //rubHulls.add(rrhull);

                    //hulls.add(rchull);
                    dichulls.put(Clus, rchull);

                    PApplet.println("Precomputed " + Clus + "/" + clusters.size());
                }
            });
            t.start();
            threads.add(t);
            // PApplet.println("hull");

            //PApplet.println("Precomputed " + c + "/" + clusters.size());

        }

        while (threads.size() > 0) {
            try {
                threads.get(0).join();
                threads.remove(0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        PApplet.println("FIM HULLS " + hulls.size());

        for (int i = 1; i <= clusters.size(); i++) {
            rubHulls.add(dicrubHulls.get(i));
            hulls.add(dichulls.get(i));
        }

        //PApplet.println("build json");

        JSONObject geo_json = buildGeoJSON();

        //PApplet.println("save json");

        saveJSONObject(geo_json, data_home + "\\" + out_path);

        /*
        oneCluster = clusters.get(0);

        rhull = new RubberHull(oneCluster.getUniquePositions(), 0.5f, 50, 2);
        for (int i = 0; i < 10000; i++) rhull.update(); // integrar isto no RubberHull

        hull = new RoundCornerHull(rhull.getVertices(), 30);
        */

        exit();
        return;

    }

    //for dbug only
    public void draw() {
        background(0, 0, 100);

        translate(width / 2, height / 2);
        scale(0.9f);
        translate(-width / 2, -height / 2);

        //rhull.update();
        //rhull.display();

        //oneCluster.display();


        //hull.setVertices(rhull.getVertices());
        //hull.display();
        //hull.displayCurve();
        //hull.displaySkeleton();


        //RoundCornerHull rh = hulls.get(round(map(mouseX, 0, width, 0, hulls.size()-1)));
        //rh.displayCurve();

        for (RubberHull rubhull : rubHulls) {
            rubhull.update();
        }

        int i = 0;
        for (RoundCornerHull rchull : hulls) {
            rchull.setVertices(rubHulls.get(i).getVertices());
            rchull.displayCurve();
            i++;
        }
    }

    public ArrayList<PVector> arrayToPVector(float[][] arr) {
        ArrayList<PVector> output = new ArrayList<PVector>();
        for (int i = 0; i < arr.length; i++) {
            output.add(new PVector(arr[i][0], arr[i][1]));
        }
        return output;
    }

    public void positionOnScreen(ZGeo helper) {
        for (Cluster clus : clusters) {
            for (Poi poi : clus.pois) {
                poi.pos = helper.toScreen(poi.latLon);
            }
        }
    }

    //reviewed
    public void loadData() {
        JSONArray json = loadJSONArray(data_home + "\\" + data_path);

        HashMap<String, Integer> repetitions = new HashMap<String, Integer>();
        for (int i = 0; i < json.size(); i++) {
            JSONObject clus_js = json.getJSONObject(i);

            Cluster clus = new Cluster();
            clus.id = clus_js.getString("id");
            clus.centroid_pos.set(clus_js.getFloat("lon"), clus_js.getFloat("lat"));

            JSONArray name_weights = clus_js.getJSONArray("name_weights");
            for (int j = 0; j < name_weights.size(); j++) {
                JSONObject nw_pair = name_weights.getJSONObject(j);
                clus.name_weights.put(nw_pair.getString("name"), nw_pair.getFloat("weight"));
            }

            JSONArray pois_js = clus_js.getJSONArray("pois");
            for (int j = 0; j < pois_js.size(); j++) {
                JSONObject poi_js = pois_js.getJSONObject(j);
                Poi poi = new Poi();
                poi.name = poi_js.getString("name");
                poi.source = poi_js.getString("source");
                double lon = poi_js.getDouble("lon");
                double lat = poi_js.getDouble("lat");
                poi.latLon.set((float) lon, (float) lat);
                poi.lat = lat;
                poi.lon = lon;

                String rep = lat + "_" + lon;

                if (!repetitions.containsKey(rep)) {
                    repetitions.put(rep, 1);
                } else {
                    repetitions.put(rep, repetitions.get(rep) + 1);
                }
                //println(poi_js);

                poi.computePoistion(map);

                clus.pois.add(poi);
            }

            clusters.add(clus);
        }

        /*
        List<Integer> vals = new ArrayList<Integer>(repetitions.values());
        Collections.sort(vals, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1 - o2;
            }
        });
        println(vals);
        println(repetitions);
        */
    }

    public JSONObject buildGeoJSON() {
        JSONObject geojson = new JSONObject();
        geojson.setString("type", "FeatureCollection");

        JSONArray features = new JSONArray();
        geojson.setJSONArray("features", features);

        //for loop
        for (int i = 0; i < hulls.size(); i++) {
            JSONObject feature = new JSONObject();
            features.append(feature);

            feature.setString("type", "Feature");
            feature.setString("id", clusters.get(i).id);


            feature.setJSONObject("properties", new JSONObject());

            JSONObject geometry = new JSONObject();
            feature.setJSONObject("geometry", geometry);

            geometry.setString("type", "Polygon");

            JSONArray coordinates1 = new JSONArray();
            JSONArray coordinates2 = new JSONArray();
            coordinates1.append(coordinates2);
            geometry.setJSONArray("coordinates", coordinates1);

            ArrayList<PVector> vertices = hulls.get(i).getOuterVertices();
            for (PVector vertex : vertices) {
                float[] os = map.screenToObjectPosition(vertex.x, vertex.y);
                ZLocation latLon = map.getTransformation().inverseTransform(os[0], os[1]);

                int cenas = 0 + 1;
                if (latLon.getLat() == Double.POSITIVE_INFINITY || latLon.getLat() == Double.NEGATIVE_INFINITY)
                    cenas++;
                else if (latLon.getLon() == Double.POSITIVE_INFINITY || latLon.getLon() == Double.NEGATIVE_INFINITY)
                    cenas++;

                try {
                    JSONArray geo_vertex = new JSONArray();
                    geo_vertex.setDouble(0, latLon.getLon());
                    geo_vertex.setDouble(1, latLon.getLat());
                    coordinates2.append(geo_vertex);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        }
        //end for loop

        return geojson;
    }

    public void saveData(JSONObject obj, String pathToSave) {

    }

    public void keyPressed() {
        if (key == 's') {
            save("result/" + PApplet.year() + PApplet.month() + PApplet.day() + PApplet.hour() + PApplet.minute() + PApplet.second() + ".png");
        } else if (key == 'r') {
            /*
            rhull = new RubberHull(arrayToPVector(points));
            int maxI = round(map(mouseX, 0, width, 10, 10000));
            for (int i = 0; i < maxI; i++) rhull.update();
            */
        }
        redraw();

    }


    public class Cluster {
        ArrayList<Poi> pois = new ArrayList<Poi>();
        String id = "";
        PVector centroid_pos = new PVector();
        HashMap name_weights = new HashMap();

        public void display() {
            noStroke();
            fill(0, 0, 0);
            for (Poi poi : pois) {
                ellipse(poi.pos.x, poi.pos.y, 4, 4);
            }
        }

        public PVector[] getCorners() {
            float lx = 999999;
            float ty = 999999;
            float rx = -99999;
            float by = -99999;
            for (Poi poi : pois) {
                if (lx > poi.latLon.x) lx = poi.latLon.x;
                else if (rx < poi.latLon.x) rx = poi.latLon.x;

                if (ty > poi.latLon.y) ty = poi.latLon.y;
                else if (by < poi.latLon.y) by = poi.latLon.y;
            }
            return new PVector[]{new PVector(lx, ty), new PVector(rx, by)};
        }

        public ArrayList<PVector> getPositions() {
            ArrayList<PVector> poss = new ArrayList<PVector>();
            for (Poi poi : pois) {
                poss.add(poi.pos);
            }
            return poss;
        }

        @Override
        public String toString() {
            String output = "";
            for (Poi poi : pois) {
                output += "[" + poi.pos.x + ", " + poi.pos.y + "], ";
            }
            return output;
        }

        ArrayList<PVector> getUniquePositions() {
            ArrayList<PVector> _pos = new ArrayList<PVector>();
            HashSet<PVector> uniquePos = new HashSet<PVector>();
            ArrayList<PVector> allPos = getPositions();

            uniquePos.addAll(allPos);
            _pos.addAll(uniquePos);

            return _pos;

//            for (PVector pt : allPos) {
//                if (uniquePos.(pt) == -1) uniquePos.add(pt);
//            }
//            return uniquePos;
//
//
//
//            HashSet<Integer> uniqueHash = new HashSet<Integer>();
//            ArrayList<PVector> uniquePos = new ArrayList<PVector>();
//            ArrayList<PVector> allPos = getPositions();
//
//            for (PVector pt : allPos) {
//                int hash = Math.round(pt.x * 10000 + pt.y * 10000 + pt.z * 10000);
//                if (!uniqueHash.contains(hash)) {
//                    uniqueHash.add(hash);
//                    uniquePos.add(pt);
//                }
//                //if (uniquePos.indexOf(pt) == -1) uniquePos.add(pt);
//            }
//            return uniquePos;
        }
    }

    class Poi {
        String name = "";
        String source = "";
        PVector pos = new PVector();
        PVector latLon = new PVector();
        double mx = 0, my = 0;
        double lon = 0, lat = 0;

        public void computePoistion(ZMap theMap) {
            //println(name);
            ZLocation geo_point = theMap.getTransformation().transform(new ZLocation(lon, lat));
            this.mx = geo_point.x;
            this.my = geo_point.y;
            //println(mx + ", "+my);
            //println(lon + ", "+lat);

            float[] transformed = theMap.objectToScreenPosition((float) mx, (float) my);
            pos.set(transformed[0], transformed[1]);
        }

        public String toString() {
            return "[" + pos.x + ", " + pos.y + "]";
        }
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

        RubberHull(ArrayList<PVector> pts, float gravity, float minLength_, float slf) {
            gravityForce = gravity;
            minLength = minLength_;
            springLengthFact = slf;

            // println(pts);
            if (pts.size() > 2) {
                Hull tempHull = new Hull(pvectorToFloat(pts));
                MPolygon region = tempHull.getRegion();
                vertices = floatToPVector(region.getCoords());
                vertices = orderPoints(vertices);
                buildEdges();
                staticPoints = clearPoints(pts, edges);
            } else if (pts.size() == 2) {
                // println(pts);
                vertices = pts;

                buildEdges();
                simulate = false;

            } else {
                PApplet.println("size = one");
                sizeOne = true;
                simulate = false;
                vertices = pts;
            }
        }

        void buildEdges() {
            // println(vertices);
            edges = new ArrayList<Edge>();
            for (int i = 0; i < vertices.size() - 1; i++) {
                edges.add(new Edge(vertices.get(i), vertices.get(i + 1), gravityForce, minLength, springLengthFact));
            }
            edges.add(new Edge(vertices.get(vertices.size() - 1), vertices.get(0), gravityForce, minLength, springLengthFact));

        }

        ArrayList<PVector> clearPoints(ArrayList<PVector> pts, ArrayList<Edge> ed) {
            ArrayList<PVector> output = new ArrayList<PVector>();
            for (PVector pt : pts) {
                boolean isAbove = false;
                for (Edge edge : ed) {
                    if (pt.equals(edge.a)) {
                        isAbove = true;
                        break;
                    }
                }
                if (!isAbove) output.add(pt);
            }
            return output;
        }

        ArrayList<PVector> orderPoints(ArrayList<PVector> pts_) {
            PVector centroid = new PVector();
            for (PVector pt : pts_) {
                centroid.add(pt);
            }
            centroid.div(pts_.size());

            ArrayList<PVector> newPts_ = new ArrayList<PVector>();

            while (!pts_.isEmpty()) {
                float smallestAngle = PConstants.TWO_PI;
                PVector biggestPoint = null;
                for (PVector v : pts_) {
                    PVector dir = PVector.sub(v, centroid);
                    float a = dir.heading() + PConstants.PI;

                    if (a <= smallestAngle) {
                        smallestAngle = a;
                        biggestPoint = v;
                    }
                }
                newPts_.add(biggestPoint);
                pts_.remove(biggestPoint);
            }
            return newPts_;
        }

        void update() {
            if (simulate) {
                ArrayList<Edge> edgesCopy = new ArrayList<Edge>();
                for (Edge edge : edges) {
                    edge.update();
                    ArrayList<Edge> newEdges = edge.checkIntersection(staticPoints);
                    if (newEdges != null) {
                        edgesCopy.addAll(newEdges);
                        staticPoints = clearPoints(staticPoints, edgesCopy);
                    } else {
                        edgesCopy.add(edge);
                    }

                }
                edges = edgesCopy;
            }
        }

        /*
        public void merge(ArrayList<Edge> newEdges){
            for(int i = 0; i < edges.size()-1; i++){
                Edge edge = edges.get(i);
                float d = PVector.dist(edge.b, newEdges.get(0).a);
                if (d == 0){
                    edges.remove(i+1);
                    edges.addAll(i+1, newEdges);
                    break;
                }
            }
            edgesToVertices();
            staticPoints = clearPoints(staticPoints);
        }
        */

        public void edgesToVertices() {
            vertices.clear();
            for (Edge edge : edges) {
                vertices.add(edge.a);
            }
        }

        ArrayList<PVector> getVertices() {
            if (!sizeOne) {
                ArrayList<PVector> outputVertices = new ArrayList<PVector>();
                for (Edge edge : edges) {
                    outputVertices.add(edge.a);
                }
                return outputVertices;
            }
            return vertices;
        }

        void display() {
            for (Edge edge : edges) {
                noFill();
                stroke(0);
                strokeWeight(1);
                ellipse(edge.a.x, edge.a.y, 15, 15);
                edge.display();

            }
            noStroke();
            fill(0);
            for (PVector pt : staticPoints) {
                ellipse(pt.x, pt.y, 2, 2);
            }
        }

        float[][] pvectorToFloat(ArrayList<PVector> pts) {
            float[][] temp = new float[pts.size()][2];
            for (int i = 0; i < temp.length; i++) {
                temp[i][0] = pts.get(i).x;
                temp[i][1] = pts.get(i).y;
            }
            return temp;
        }

        ArrayList<PVector> floatToPVector(float[][] arr) {
            ArrayList<PVector> output = new ArrayList<PVector>();
            for (int i = 0; i < arr.length; i++) {
                output.add(new PVector(arr[i][0], arr[i][1]));
            }
            return output;
        }
    }


    public class Edge {
        PVector a, b;
        PushPoint pushPoint;
        float length = 0;
        float minDifference = 5;

        // pushPoint properties
        float gravityForce, minLength, springLengthFact;

        Edge(PVector a_, PVector b_, float gravity, float minLength_, float slf) {
            // println(a_ + " " + b_);
            a = a_;
            b = b_;
            gravityForce = gravity;
            minLength = minLength_;
            springLengthFact = slf;
            length = PVector.dist(a, b);

            PVector dif = PVector.sub(b, a);
            dif.mult(0.5f);
            PVector centerPos = a.get();
            centerPos.add(dif);

            PVector perp = new PVector(-dif.y, dif.x);
            ;
            perp.normalize();

            pushPoint = new PushPoint(centerPos, perp, length, gravityForce, minLength, springLengthFact);
        }

        public void update() {
            pushPoint.update();
        }

        ArrayList<Edge> checkIntersection(ArrayList<PVector> staticPts) {
            ArrayList<PVector> nextPoints = filterPoints(staticPts);

            if (nextPoints.size() != 0) {
                ArrayList<Edge> newEdges = new ArrayList<Edge>();
                PVector clossestPoint = orderPoints(nextPoints);
                newEdges.add(new Edge(a, clossestPoint, gravityForce, minLength, springLengthFact));
                newEdges.add(new Edge(clossestPoint, b, gravityForce, minLength, springLengthFact));
                return newEdges;
            }
            return null;
        }

        ArrayList<PVector> filterPoints(ArrayList<PVector> staticPts) {
            ArrayList<PVector> nextPoints = new ArrayList<PVector>();

            for (PVector pt : staticPts) {
                if (pointInTriangleCheck(pt, a, pushPoint.pos, b)) {
                    nextPoints.add(pt);
                }
            }
            return nextPoints;
        }

        boolean pointInTriangleCheck(PVector pt, PVector a, PVector b, PVector c) {
            PVector avec = PVector.sub(b, a);
            PVector ap = PVector.sub(pt, a);
            PVector bvec = PVector.sub(c, b);
            PVector bp = PVector.sub(pt, b);
            PVector cvec = PVector.sub(a, c);
            PVector cp = PVector.sub(pt, c);
            boolean ua = (avec.cross(ap).z < 0f);
            boolean ub = (bvec.cross(bp).z < 0f);
            boolean uc = (cvec.cross(cp).z < 0f);

            return ua && ub && uc;
        }

        PVector orderPoints(ArrayList<PVector> pts) {
            float smallestDistance = 99999f;
            PVector clossestPoint = null;
            for (PVector v : pts) {
                float d = PVector.dist(v, pushPoint.iniPoint);
                if (d < smallestDistance) {
                    smallestDistance = d;
                    clossestPoint = v;
                }
            }
            return clossestPoint;
        }

        // for debug proposes
        public void displayArrow() {
            stroke(0);
            line(-5, -3, 0, 0);
            line(-5, 3, 0, 0);
        }

        void display() {
            noFill();
            stroke(0);
            strokeWeight(1);
            line(a.x, a.y, b.x, b.y);
            strokeWeight(0.5f);
            pushPoint.display();
            stroke(0, 50, 80);
            line(a.x, a.y, pushPoint.pos.x, pushPoint.pos.y);
            line(pushPoint.pos.x, pushPoint.pos.y, b.x, b.y);

            PVector dir1 = PVector.sub(pushPoint.pos, a);
            PVector dir2 = PVector.sub(b, pushPoint.pos);

            float ang1 = dir1.heading();
            float ang2 = dir2.heading();

            strokeWeight(0.5f);
            pushMatrix();
            translate(pushPoint.pos.x, pushPoint.pos.y);
            rotate(ang1);
            displayArrow();
            popMatrix();

            pushMatrix();
            translate(b.x, b.y);
            rotate(ang2);
            // displayArrow();
            popMatrix();

            PVector dir3 = PVector.sub(b, a);
            float ang3 = dir3.heading();

            strokeWeight(2);
            pushMatrix();
            translate(b.x, b.y);
            rotate(ang3);
            displayArrow();
            popMatrix();

            noStroke();
            fill(0);
            rect(pushPoint.iniPoint.x, pushPoint.iniPoint.y, 5, 5);

        }

        public PVector getPerpendicular() {
            PVector dif = PVector.sub(b, a);
            return new PVector(-dif.y, dif.x);
        }
    }

    class PushPoint {
        PVector pos, force, iniPoint, gravityVec, resistanceVec;
        float length;
        float gravityForce, minLength, maxSrpingLength, springLengthFact;

        PushPoint(PVector pos_, PVector dir_, float length_, float gravity, float minLength_, float slf) {
            gravityForce = gravity;
            minLength = minLength_;
            springLengthFact = slf;
            pos = pos_.get();
            iniPoint = pos_.get();
            length = length_;
            maxSrpingLength = length * springLengthFact;

            gravityVec = dir_.get();
            gravityVec.normalize();
            gravityVec.mult(gravityForce);

            force = new PVector();
            update();
        }

        //debug
        void display() {
            pushMatrix();
            translate(pos.x, pos.y);
            PVector temp = force.get();
            temp.mult(10);
            noStroke();
            fill(0, 50, 80);
            ellipse(0, 0, 2, 2);
            noFill();
            stroke(0, 50, 80);
            strokeWeight(0.5f);
            line(0, 0, temp.x, temp.y);
            popMatrix();
        }

        void update() {
            float resistance = PApplet.constrain(minLength / length, 0, gravityForce);
            resistanceVec = gravityVec.get();
            resistanceVec.normalize();
            resistanceVec.mult(-1);
            resistanceVec.mult(resistance);

            float d = PVector.dist(pos, iniPoint);
            float springResistance = PApplet.map(d, 0, maxSrpingLength, 1, 0);

            force.mult(0);
            force.add(gravityVec);
            force.add(resistanceVec);
            force.mult(springResistance);

            pos.add(force);
        }
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

        // RoundCornerHull(ArrayList<Segment> input, float shpWidth){
        // 	innerShape = input;
        // 	this.shpWidth = shpWidth;
        // 	outerShape = new ArrayList<Segment>();
        // 	outerParallel = new ArrayList<Segment>();
        // 	corners = new ArrayList<Corner>();
        // 	outerVertices = new ArrayList<PVector>();
        // 	build(ADAPTIVE); // <-- outer hull is built here
        // }

        RoundCornerHull(ArrayList<PVector> inputVertices, float shpWidth) {
            innerShape = toSegments(inputVertices);
            this.shpWidth = shpWidth;
            outerShape = new ArrayList<Segment>();
            outerParallel = new ArrayList<Segment>();
            corners = new ArrayList<Corner>();
            outerVertices = new ArrayList<PVector>();
            build(ADAPTIVE); // <-- outer hull is built here
        }

        //Transforms array of vertices to array of segments
        ArrayList<Segment> toSegments(ArrayList<PVector> inputVertices) {
            ArrayList<Segment> output = new ArrayList<Segment>();
            for (int i = 0; i < inputVertices.size() - 1; i++) {
                output.add(new Segment(inputVertices.get(i), inputVertices.get(i + 1)));
            }
            output.add(new Segment(inputVertices.get(inputVertices.size() - 1), inputVertices.get(0)));
            return output;
        }

        //Build the outer hull
        private void build(int mode) {
            outerShape.clear();
            outerParallel.clear();
            corners.clear();
            outerVertices.clear();
            // 1st phase -> compute outer parallel segments
            for (int i = 0; i < innerShape.size(); i++) {
                Segment innerSegA = innerShape.get(i);
                outerParallel.add(innerSegA.getParallel(shpWidth, false));
            }

            // 2nd phase -> compute corners
            boolean pass = false;
            int offset = 0;
            int k = 0;
            int i = 0;
            while (k < outerParallel.size()) {
                Segment segA = outerParallel.get(i);
                Segment segB = null;
                if (i < outerParallel.size() - 1) segB = outerParallel.get(i + 1);
                else segB = outerParallel.get(0);

                PVector iPoint = ZGeometry.intersectingPoint(segA.a, segA.b, segB.a, segB.b);
                if (iPoint == null) {
                    Segment inSegA = innerShape.get(i);
                    ArrayList<PVector> outIpoints = new ArrayList<PVector>();
                    outIpoints.add(inSegA.b);

                    ArrayList<Segment> outSegments = new ArrayList<Segment>();
                    outSegments.add(segA);
                    outSegments.add(segB);
                    corners.add(new Corner(outSegments, outIpoints, Corner.OPEN, shpWidth));
                    pass = true;
                } else if (iPoint != null && pass) {
                    ArrayList<Segment> outSegments = new ArrayList<Segment>();
                    ArrayList<PVector> outIpoints = new ArrayList<PVector>();
                    outSegments.add(segA);
                    boolean isAcute = true;
                    int j = i;
                    while (isAcute) {
                        Segment segAa = outerParallel.get(j);
                        Segment segBb;
                        if (j < outerParallel.size() - 1) segBb = outerParallel.get(j + 1);
                        else segBb = outerParallel.get(0);

                        PVector dirAa = segAa.getDirectionInverse();
                        PVector dirBb = segBb.getDirection();

                        float crossProd = dirAa.cross(dirBb).z;
                        PVector outIPoint = ZGeometry.intersectingPoint(segAa.a, segAa.b, segBb.a, segBb.b);

                        if (outIPoint == null) {
                            k += outSegments.size() - 2;
                            corners.add(new Corner(outSegments, outIpoints, Corner.CLOSED, shpWidth));
                            isAcute = false;
                        } else {
                            outSegments.add(segBb);
                            outIpoints.add(outIPoint);
                        }
                        j = (j < outerParallel.size() - 1) ? j + 1 : 0;
                    }
                } else {
                    offset++;
                    i++;
                }

                if (pass) {
                    k++;
                    i = k + offset;
                    if (i >= outerParallel.size()) {
                        offset = -k;
                        i = 0;
                    }
                }
            }

            // 3rd phase -> build rounded corners
            for (Corner corner : corners) {
                corner.compute();
            }

            for (Corner corner : corners) {
                outerVertices.addAll(corner.getVertices());
            }
            outerShape = toSegments(outerVertices);
        }

        //For debug purposes
        void display() {
            noFill();
            ellipseMode(PConstants.RADIUS);
            strokeWeight(2);
            stroke(0);
            for (Segment seg : innerShape) {
                line(seg.a.x, seg.a.y, seg.b.x, seg.b.y);
            }

            stroke(0);
            strokeWeight(2);
            for (Segment seg : outerParallel) {
                line(seg.a.x, seg.a.y, seg.b.x, seg.b.y);
            }

            for (Corner corner : corners) {
                corner.display();
            }
        }

        void displayCurve() {
            noFill();
            stroke(0);
            strokeWeight(1);
            beginShape();
            // curveVertex(outerVertices.get(0).x, outerVertices.get(0).y);
            for (PVector seg : outerVertices) {
                vertex(seg.x, seg.y);
            }
            // curveVertex(outerVertices.get(outerVertices.size()-1).x, outerVertices.get(outerVertices.size()-1).y);
            endShape(PConstants.CLOSE);

        }

        void displaySkeleton() {
            noFill();
            ellipseMode(PConstants.RADIUS);
            dsplSkltn = true;

            stroke(150, 180, 0);
            strokeWeight(1);
            for (Segment seg : innerShape) {
                ellipse(seg.a.x, seg.a.y, shpWidth, shpWidth);
            }

            // for (Segment seg : outerParallel){
            // 	line(seg.a.x, seg.a.y, seg.b.x, seg.b.y);
            // }

            fill(0);
            noStroke();
            for (Segment seg : outerShape) {
                ellipse(seg.b.x, seg.b.y, 2, 2);
            }
        }

        public void setVertices(ArrayList<PVector> inputVertices) {
            innerShape = toSegments(inputVertices);
            build(ADAPTIVE); // <-- outer hull is built here
        }

        public ArrayList<PVector> getVertices() {
            ArrayList<PVector> vertices = new ArrayList<PVector>();
            for (Segment seg : outerShape) {
                vertices.add(seg.a);
            }
            return vertices;
        }

        public ArrayList<PVector> getInnerVertices() {
            ArrayList<PVector> vert = new ArrayList<PVector>();
            for (Segment seg : innerShape) {
                vert.add(seg.a);
            }
            return vert;
        }

        public ArrayList<PVector> getOuterVertices() {
            return outerVertices;
        }

        PPath getPath() {
            PPath pth = new PPath();

            for (Segment seg : outerShape) {
                pth.add(seg);
            }
            return pth;
        }
    }

    class Segment {
        PVector a, b;
        float offset = 0;
        float startAngleSmooth = 1; // [0, 1]
        float absAngSmooth = 5; // [0, length()/2]

        Segment(PVector a_, PVector b_) {
            a = a_;
            b = b_;
        }

        public float length() {
            return PVector.dist(a, b);
        }

        public PVector getPointCoords(float relPos) {
            PVector dir = direction();
            dir.setMag(relPos - offset);
            dir.add(a);
            return dir;
            // return PVector.lerp(a, b, (relPos-offset)/length());
        }

        public float getAngle() {
            return direction().heading();
        }

        public float getSmoothAngle(float relPos, Segment prev, Segment next) {
            // if prev != null and next != null
            float relSegPos = relPos - offset;
            absAngSmooth = PApplet.constrain(absAngSmooth, 0, length() / 2f);
            if (relSegPos > absAngSmooth && relSegPos < length() - absAngSmooth) return getAngle();

            float ang = getAngle();
            PVector dir = direction();
            if (relSegPos < absAngSmooth && prev != null) {
                PVector prevDir = prev.direction();
                float angBetween = PVector.angleBetween(direction(), prevDir);

                boolean isAcute = (prevDir.cross(dir).z < 0);
                float distAng = isAcute ? ang + angBetween / 2f : ang - angBetween / 2f;
                float finAng = PApplet.map(relSegPos, absAngSmooth, 0, ang, distAng);

                return finAng;
            } else if (relSegPos > length() - absAngSmooth && next != null) {
                PVector nextDir = next.direction();
                float angBetween = PVector.angleBetween(direction(), nextDir);

                boolean isAcute = (dir.cross(nextDir).z < 0);
                float distAng = isAcute ? ang - angBetween / 2f : ang + angBetween / 2f;
                float finAng = PApplet.map(relSegPos, length() - absAngSmooth, length(), ang, distAng);

                return finAng;
            }

            return getAngle();
        }

        Segment getParallel(float distance, boolean right) {
            PVector dirVec = PVector.sub(b, a);
            PVector perpendicular = new PVector();
            if (right) {
                perpendicular.x = -dirVec.y;
                perpendicular.y = dirVec.x;
            } else {
                perpendicular.x = dirVec.y;
                perpendicular.y = -dirVec.x;
            }
            perpendicular.normalize();
            perpendicular.mult(distance);
            PVector newA = perpendicular.get();
            newA.add(a);
            PVector newB = perpendicular.get();
            newB.add(b);
            return new Segment(newA, newB);
        }

        void angleBetween(Segment other) {

        }

        Segment get() {
            return new Segment(a.get(), b.get());
        }

        PVector getDirection() {
            return PVector.sub(b, a);
        }

        public PVector direction() {
            return PVector.sub(b, a);
        }

        PVector getDirectionInverse() {
            return PVector.sub(a, b);
        }
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

        Corner(ArrayList<Segment> inSegments_, ArrayList<PVector> iPoints_, int type, float shpWidth_) {
            inSegments = inSegments_;
            iPoints = iPoints_;
            shpWidth = shpWidth_;
            this.type = type;
            segments = new ArrayList<Segment>();
        }

        void display() {
            fill(0);
            noStroke();
            for (PVector iPoint : iPoints) {
                ellipse(iPoint.x, iPoint.y, 6, 6);
            }
        }

        void compute() {
            if (type == OPEN) computeOpen();
            else if (type == CLOSED) computeClosed();
        }

        void computeOpen() {
            segments = new ArrayList<Segment>();

            Segment segA = inSegments.get(0);
            Segment segB = inSegments.get(1);
            PVector iPoint = iPoints.get(0);

            PVector dirA = PVector.sub(segA.a, segA.b);
            PVector dirB = PVector.sub(segB.b, segB.a);

            float beta = PVector.angleBetween(dirA, dirB);
            float alpha = PConstants.PI - beta;
            float arcLength = shpWidth * alpha;

            if (arcLength < arcLengthStep) {
                segments.add(new Segment(segA.b, segB.a));
            } else {
                int numDiv = PApplet.round(arcLength / arcLengthStep);
                float angleStep = alpha / (float) (numDiv);
                // println(numDiv);
                float startAngle = dirA.heading() + PConstants.TWO_PI + PConstants.HALF_PI;
                PVector prev = segA.b;
                for (int i = 1; i < numDiv; i++) {
                    float actAng = startAngle + angleStep * i;
                    float x = PApplet.cos(actAng) * shpWidth;
                    float y = PApplet.sin(actAng) * shpWidth;
                    PVector actual = new PVector(x, y);
                    actual.add(iPoint);
                    segments.add(new Segment(prev, actual));
                    prev = actual;
                }
                segments.add(new Segment(prev, segB.a));
            }
            // println(dirA + " | " + dirB + " | " + degrees(alphaAngle));
            // return segments;
        }

        void renderCross(float x, float y, float dim) {
            noFill();
            stroke(0);
            strokeWeight(0.75f);
            line(x - dim / 2, y - dim / 2, x + dim / 2, y + dim / 2);
            line(x - dim / 2, y + dim / 2, x + dim / 2, y - dim / 2);
        }

        void computeClosed() {

            if (inSegments.size() != 0) {
                // step 1 - construct array with bezier anchors and control points
                ArrayList<PVector> bezierDescription = new ArrayList<PVector>();
                PVector middle = PVector.sub(iPoints.get(0), inSegments.get(0).a);
                middle.mult(0.5f);
                middle.add(inSegments.get(0).a);
                bezierDescription.add(middle);
                for (int i = 1; i < inSegments.size(); i++) {
                    PVector iPoint = iPoints.get(i - 1);
                    bezierDescription.add(iPoint);
                    bezierDescription.add(iPoint);
                    PVector sgmB = inSegments.get(i).b;
                    PVector mdl = PVector.sub(sgmB, iPoint);
                    mdl.mult(0.5f);
                    mdl.add(iPoint);
                    bezierDescription.add(mdl);
                }
                for (PVector vec : bezierDescription) {
                    noStroke();
                    fill(0);
                    // renderCross(vec.x, vec.y, 15);
                }

                // step 2 - subdivide bezier in segments
                int numSeg = 20;
                ArrayList<PVector> intermediate = new ArrayList<PVector>();
                for (int i = 1; i < numSeg; i++) {
                    intermediate.add(computePointOnBezier(i / (float) numSeg, bezierDescription));
                }

                segments.add(new Segment(inSegments.get(0).a, intermediate.get(0)));
                for (int i = 0; i < intermediate.size() - 1; i++) {
                    segments.add(new Segment(intermediate.get(i), intermediate.get(i + 1)));
                }
                segments.add(new Segment(intermediate.get(intermediate.size() - 1), inSegments.get(inSegments.size() - 1).b));
            }
        }

        PVector computePointOnBezier(float t, ArrayList<PVector> cp) {
            PVector output = new PVector();
            for (int i = 0; i < cp.size(); i++) {
                PVector pn = cp.get(i).get();
                pn.mult(bernstein(t, i, cp.size() - 1));
                output.add(pn);
            }

            return output;
        }

        float bernstein(float t, int i, int n) {
            float bin = (i == 0 || i == n) ? 1 : factorial(n) / (factorial(i) * factorial(n - i));
            return bin * PApplet.pow(t, i) * PApplet.pow(1 - t, n - i);
        }

        float factorial(int n) {
            float fact = n;
            for (int i = n - 1; i > 0; i--) fact *= i;
            return fact;
        }

        ArrayList<Segment> getSegments() {
            ArrayList<Segment> output = new ArrayList<Segment>();
            if (segments.size() > 0) {
                output.add(new Segment(inSegments.get(0).a, segments.get(0).a));
                for (Segment segment : segments) {
                    output.add(segment);
                }
            }

            return output;
        }

        ArrayList<PVector> getVertices() {
            ArrayList<PVector> vertices = new ArrayList<PVector>();
            for (Segment seg : segments) {
                vertices.add(seg.a);
            }
            vertices.add(segments.get(segments.size() - 1).b);
            return vertices;
        }

    }


    class PPath extends ArrayList<Segment> {
        float totalLength = 0;
        float normPosition = 0.0f;
        float absPosition = 0.0f;
        String id;
        boolean haveSpace = true;

        public PPath() {
            super();
        }

        public PPath(ArrayList<Segment> path_) {
            super(path_);
        }

        public boolean add(Segment item) {
            item.offset = length();
            totalLength += item.length();
            return super.add(item);
        }

        public float length() {
            return totalLength;
        }

        public PVector getPointCoords(float normPoint) { // [0, length]
            float relativePoint = normPoint;
            for (Segment seg : this) {
                if (seg.offset <= relativePoint && relativePoint < (seg.offset + seg.length())) {
                    return seg.getPointCoords(relativePoint);
                }
            }

            // if (relativePoint < 0) return this.get(0).a;
            // return this.get(this.size()-1).b;
            return null;
        }

        public float getTillAng(float normPoint) { // [0, length]
            float relativePoint = normPoint;
            for (int i = 0; i < this.size(); i++) {
                Segment seg = this.get(i);
                if (seg.offset <= relativePoint && relativePoint < (seg.offset + seg.length())) {
                    if (i == 0) return seg.getSmoothAngle(relativePoint, null, this.get(i + 1));
                    else if (i == this.size() - 1) return seg.getSmoothAngle(relativePoint, this.get(i - 1), null);
                    return seg.getSmoothAngle(relativePoint, this.get(i - 1), this.get(i + 1));
                }
            }

            if (relativePoint < 0) return this.get(0).direction().heading();
            return this.get(this.size() - 1).direction().heading();
        }

        PShape getShape() {
            PShape shp = createShape();
            shp.beginShape();
            shp.noFill();
            shp.stroke(30, 30, 40);
            shp.strokeWeight(2);
            for (Segment seg : this) {
                shp.vertex(seg.a.x, seg.a.y);
            }
            shp.endShape(PConstants.CLOSE);
            return shp;
        }

        float normalToAbsolute() {
            return absPosition / totalLength;
        }

        float absoluteToNormal() {
            return totalLength * normPosition;
        }

        void rewind() {
            normPosition = 0.0f;
            absPosition = 0.0f;
            haveSpace = true;
        }

        void addPosition(float pos_) {
            absPosition += pos_;
        }

        void subPosition(float pos_) {
            absPosition -= pos_;
            if (absPosition < 0) absPosition = 0;
        }

        boolean hasSpace() {
            if (absPosition < totalLength) return true;
            return false;
        }
    }


    public class RubberRunnable implements Runnable {

        RubberClusters _rbc;
        RubberClusters.Cluster _cluster;

        public RubberRunnable(RubberClusters rbc, RubberClusters.Cluster cluster) {
            _rbc = rbc;
            _cluster = cluster;

        }

        @Override
        public void run() {

            RubberClusters.RubberHull rrhull = new RubberClusters.RubberHull(_cluster.getUniquePositions(), 0.5f, 10f, 2f); //0.1, 10, 2
            for (int i = 0; i < 10000; i++) {

                //PApplet.print(".");
                rrhull.update();
            }
            RubberClusters.RoundCornerHull rchull = new RubberClusters.RoundCornerHull(rrhull.getVertices(), 10f);
            _rbc.rubHulls.add(rrhull);
            _rbc.hulls.add(rchull);
        }
    }


    static public void Run(String[] passedArgs) {
        String[] appletArgs = new String[]{"pt.isec.fc.RubberClusters"};
        if (passedArgs != null) {
            PApplet.main(PApplet.concat(appletArgs, passedArgs));
        } else {
            PApplet.main(appletArgs);
        }
    }
}
