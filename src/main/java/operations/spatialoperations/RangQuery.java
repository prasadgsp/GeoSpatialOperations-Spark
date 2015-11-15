package edu.asu.cse512;

import java.util.List;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class RangQuery 
{
	public static void main( String[] args )
	{
		SparkConf conf = new SparkConf().setAppName("Group28-RangeQuery");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> input = sc.textFile(args[0]);
		JavaRDD<Polygon> window = sc.textFile(args[1]).map(new Function<String, Polygon>(){
			public Polygon call(String s)
			{
				String[] points = s.split(",");
				GeometryFactory factory = new GeometryFactory();
				Coordinate[] co = new Coordinate[]{new Coordinate(Double.parseDouble(points[0]),Double.parseDouble(points[1])), new Coordinate(Double.parseDouble(points[2]),Double.parseDouble(points[1])),new Coordinate(Double.parseDouble(points[2]),Double.parseDouble(points[3])),	new Coordinate(Double.parseDouble(points[0]),Double.parseDouble(points[3])), new Coordinate(Double.parseDouble(points[0]),Double.parseDouble(points[1]))};
				Polygon win = factory.createPolygon(co);
				return win;
			}});

		List<Polygon> list = window.collect();		
		final Broadcast<List<Polygon>> bwindow = sc.broadcast(list);
		JavaRDD<Integer> mapped = input.map(new Function<String, Integer>(){
			public Integer call(String s) 
			{
				String[] points = s.split(",");
				GeometryFactory factory = new GeometryFactory();
				Point p = factory.createPoint(new Coordinate(Double.parseDouble(points[1]),Double.parseDouble(points[2])));
				List<Polygon> windows = bwindow.getValue();
				Polygon r = windows.get(0);

				if(r.contains(p))
				{
					return Integer.parseInt(points[0]);
				}else
				{
					return null;
				}
			}
		});

		JavaRDD<Integer> res = mapped.filter(new Function<Integer, Boolean>(){
			public Boolean call(Integer s) {return (s != null);}});
		
		JavaRDD<Integer> output = res.sortBy(new Function<Integer, Integer>(){
			public Integer call(Integer a)
			{
				return a;
			}
			
		}, true, 1).coalesce(1);
		output.saveAsTextFile(args[2]);

	}
}
