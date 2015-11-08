package operations.spatialoperations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

public class GeometryUnion {

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		SparkConf spark = new SparkConf().setAppName("App").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(spark);
		JavaRDD<String> textFile = sc.textFile(args[0]);
		JavaRDD<String> geoCoordinates = textFile.map(new Function<String,String>(){
			public String call(String str){
				if(str.isEmpty() || str.contains("x") || str.contains("y")){
					return "";
				}
				String[] geoLatLong = str.split(",");
				return Arrays.toString(geoLatLong);
			}
		});

		JavaRDD<Geometry> geoPolygons = geoCoordinates.mapPartitions(new FlatMapFunction<Iterator<String>, Geometry>() {
			public Iterable<Geometry> call(Iterator<String> geoPoints){
				List<Geometry> createdGeoPolygons = new ArrayList<Geometry>();
				GeometryFactory factory = new GeometryFactory();
				String instance;
				while(geoPoints.hasNext()){
					instance = geoPoints.next();
					if(!instance.isEmpty()){
						Coordinate[] geoPolygonCoordinates = getParseGeoCoordinates(instance);
						Polygon geoPolygon = factory.createPolygon(geoPolygonCoordinates);

						createdGeoPolygons.add(geoPolygon);
					}
				}

				return createdGeoPolygons;
			}
		});

		JavaRDD<Geometry> geoPolygonUnion = geoPolygons.mapPartitions(new FlatMapFunction<Iterator<Geometry>, Geometry>(){
			public Iterable<Geometry> call(Iterator<Geometry> geoPolygonsList){
				List<Geometry> geoPolygons = new ArrayList<Geometry>();
				Geometry geoElement;
				while(geoPolygonsList.hasNext()){
					geoElement = geoPolygonsList.next();
					geoPolygons.add(geoElement);
				}

				List<Geometry> geoUnionPolygons = new ArrayList<Geometry>();

				int index = -1;
				while(!geoPolygons.isEmpty()){
					index++;
					geoElement = geoPolygons.get(index);
					geoPolygons.remove(index);
					while(index < geoPolygons.size() && !geoPolygons.isEmpty()){
						if(geoElement.intersects(geoPolygons.get(index))){
							geoElement = geoElement.union(geoPolygons.get(index));
							geoPolygons.remove(index);
							index = 0;
						}
						else{
							index++;
						}
					}
					geoUnionPolygons.add(geoElement);
					index = -1;
				}
				
				return geoUnionPolygons;
			}
		});

		JavaRDD<List<Geometry>> geoPolygonLists = geoPolygonUnion.glom();
		
		List<Geometry> geoReduceUnionPolygons = geoPolygonLists.reduce(new Function2<List<Geometry>, List<Geometry>, List<Geometry>>(){
			public List<Geometry> call(List<Geometry> List1, List<Geometry> List2){
				List<Geometry> geoPolygons = new ArrayList<Geometry>();
				List<Geometry> Lists = new ArrayList<Geometry>();
				Lists.addAll(List1);
				Lists.addAll(List2);
				Iterator<Geometry> geoPolygonsListIterator = Lists.iterator();
				Geometry geoElement;
				while(geoPolygonsListIterator.hasNext()){
					geoElement = geoPolygonsListIterator.next();
					geoPolygons.add(geoElement);
				}

				List<Geometry> geoUnionPolygons = new ArrayList<Geometry>();

				int index = -1;
				while(!geoPolygons.isEmpty()){
					index++;
					geoElement = geoPolygons.get(index);
					geoPolygons.remove(index);
					while(index < geoPolygons.size() && !geoPolygons.isEmpty()){
						if(geoElement.intersects(geoPolygons.get(index))){
							geoElement = geoElement.union(geoPolygons.get(index));
							geoPolygons.remove(index);
							index = 0;
						}
						else{
							index++;
						}
					}
					geoUnionPolygons.add(geoElement);
					index = -1;
				}
				
				return geoUnionPolygons;
			}
		});
		
		
		JavaRDD<Geometry> PolygonsOutput = sc.parallelize(geoReduceUnionPolygons);

		JavaRDD<Geometry> geoUnionPolygonsOutput = PolygonsOutput.coalesce(1);
		
		geoUnionPolygonsOutput.saveAsTextFile(args[1]);
		sc.close();

	}

	public static Coordinate[] getParseGeoCoordinates(String instance){
		Double lat1 = Double.parseDouble(instance.split("\\s*,\\s*")[0].substring(1, instance.split("\\s*,\\s*")[0].length()));
		Double long1 = Double.parseDouble(instance.split("\\s*,\\s*")[1]);
		Double lat2 = Double.parseDouble(instance.split("\\s*,\\s*")[2]);
		Double long2 = Double.parseDouble(instance.split("\\s*,\\s*")[3].substring(0, instance.split("\\s*,\\s*")[3].length()-1));

		Coordinate[] geoPolygonCoordinates = new Coordinate[5];
		geoPolygonCoordinates[0] = new Coordinate(lat1,long1);
		geoPolygonCoordinates[1] = new Coordinate(lat1,long2);
		geoPolygonCoordinates[2] = new Coordinate(lat2,long2);
		geoPolygonCoordinates[3] = new Coordinate(lat2,long1);
		geoPolygonCoordinates[4] = new Coordinate(lat1,long1);

		return geoPolygonCoordinates;
	}

}
