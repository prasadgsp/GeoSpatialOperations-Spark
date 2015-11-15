package edu.asu.cse512;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;

public class Join 
{
	public static void main( String[] args )
	{
		SparkConf conf = new SparkConf().setAppName("Group28-Join");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Tuple2<String, Polygon>> window = sc.textFile(args[1]).map(new Function<String,Tuple2<String, Polygon> >(){
			public Tuple2<String, Polygon> call(String s)
			{
				String[] points = s.split(",");
				GeometryFactory factory = new GeometryFactory();
				Coordinate[] co = new Coordinate[]{new Coordinate(Double.parseDouble(points[1]),Double.parseDouble(points[2])), new Coordinate(Double.parseDouble(points[3]),Double.parseDouble(points[2])),new Coordinate(Double.parseDouble(points[3]),Double.parseDouble(points[4])),	new Coordinate(Double.parseDouble(points[1]),Double.parseDouble(points[4])), new Coordinate(Double.parseDouble(points[1]),Double.parseDouble(points[2]))};
				Polygon p = factory.createPolygon(co);
				Tuple2<String, Polygon> res = new Tuple2<String, Polygon>(points[0], p);
				return res;
			}
		}
				);
		final Broadcast<List<Tuple2<String, Polygon>>> windows = sc.broadcast(window.collect());
		JavaRDD<String> compare = sc.textFile(args[0]);
		String[] cmp = compare.take(1).toString().split(",");
		if(cmp.length == 3)			
		{
			JavaRDD<String> inputPoints = sc.textFile(args[0]);
			JavaPairRDD<String, String> mappedPoints = inputPoints.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, String>()

					{
				public Iterable<Tuple2<String, String>> call(Iterator<String> s)
				{
					ArrayList<Tuple2<String, String>> list = new ArrayList<Tuple2<String,String>>();
					Point p = null;   
					GeometryFactory factory = null;    			
					String[] points = null;

					List<Tuple2<String, Polygon>> localwindows = windows.value();
					while(s.hasNext())
					{
						points = s.next().split(","); 
						factory = new GeometryFactory();
						p = factory.createPoint(new Coordinate(Double.parseDouble(points[1]),Double.parseDouble(points[2])));

						for(Tuple2<String, Polygon> rect : localwindows)
						{ 	
							if(rect._2.contains(p))
							{	
								list.add(new Tuple2<String, String>(rect._1, points[0]));     
							}
						}
					}
					return list;
				}
					});
			JavaPairRDD<String, Iterable<String>> resPoints = mappedPoints.groupByKey().sortByKey();
			resPoints.map(new Function<Tuple2<String, Iterable<String>>, String>(){
				public String call(Tuple2<String, Iterable<String>> t)
				{
					List<Integer> temp = new ArrayList<Integer>();

					String res = "";
					for(String s : t._2())
					{
						temp.add(Integer.parseInt(s));
					}
					Collections.sort(temp);
					return t._1 + "," + temp.toString().substring(1 , temp.toString().length()-1).replace(" ", "");
				}
			}).coalesce(1).saveAsTextFile(args[2]);
		}
		else
		{
			JavaRDD<String> inputRectangles = sc.textFile(args[0]);
			JavaPairRDD<String, String> mappedRectangles = inputRectangles.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, String>()
					{
				public Iterable<Tuple2<String, String>> call(Iterator<String> s)
				{
					ArrayList<Tuple2<String, String>> list = new ArrayList<Tuple2<String,String>>();        		
					Polygon r = null;
					GeometryFactory factory = null;
					Coordinate[] co = null;
					String[] points = null;

					List<Tuple2<String, Polygon>> localwindows = windows.value();
					while(s.hasNext())
					{
						points = s.next().split(",");
						factory = new GeometryFactory();
						co = new Coordinate[]{new Coordinate(Double.parseDouble(points[1]),Double.parseDouble(points[2])), new Coordinate(Double.parseDouble(points[3]),Double.parseDouble(points[2])),new Coordinate(Double.parseDouble(points[3]),Double.parseDouble(points[4])),	new Coordinate(Double.parseDouble(points[1]),Double.parseDouble(points[4])), new Coordinate(Double.parseDouble(points[1]),Double.parseDouble(points[2]))};
						r = factory.createPolygon(co);

						for(Tuple2<String, Polygon> rect : localwindows)
						{ 	
							if(rect._2.intersects(r) || rect._2.contains(r))
							{
								list.add(new Tuple2<String, String>(rect._1, points[0]));
							}

						}
					}
					return list;
				}
					});

			JavaPairRDD<String, Iterable<String>> resRectangles = mappedRectangles.groupByKey().sortByKey();



			resRectangles.map(new Function<Tuple2<String, Iterable<String>>, String>(){
				public String call(Tuple2<String, Iterable<String>> t)
				{
					List<Integer> temp = new ArrayList<Integer>();

					String res = "";
					for(String s : t._2())
					{
						temp.add(Integer.parseInt(s));
					}
					Collections.sort(temp);
					return t._1 + "," + temp.toString().substring(1 , temp.toString().length()-1).replace(" ", "");
				}
			}).coalesce(1).saveAsTextFile(args[2]); 
		}
	}          


}
