package operations.spatialoperations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class ConvexHull 
{
	@SuppressWarnings("serial")
	// Convert input data into set of Points
	public static class getPoints implements Function<String, Point> 
	{
	    public Point call(String inputLines) 
	    {
	      String coordinates[] = inputLines.split("\\s*,\\s*");
	      Point point = new Point(Double.parseDouble(coordinates[0]),Double.parseDouble(coordinates[1]));
	      return  point;
	    }
	  }
	
	// Compute local convex hull at each of the partitions
	public static class computeLocalConvexHull implements FlatMapFunction <Iterator<Point> ,Point>
	{ 
		private static final long serialVersionUID = 1L;

		public Iterable<Point> call(Iterator<Point> inputPoints) throws Exception 
		{
			ArrayList<Point> pointsList = new ArrayList<Point>();
			while(inputPoints.hasNext())
			{
				pointsList.add(inputPoints.next());
			}
			
			QuickHull computeConvexHull = new QuickHull();
			
			ArrayList<Point> localHullPointList = computeConvexHull.quickHull(pointsList);
			return localHullPointList;
		}
	}
	
	public static void main(String args[]) throws Exception
	{
		String inputFile = args[0];
	    String outputFile = args[1];
	    
	    SparkConf conf = new SparkConf().setAppName("operations.spatialoperations.ConvexHull");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    JavaRDD<String> input = sc.textFile(inputFile);
	    JavaRDD<Point> inputPoints = input.map(new getPoints());
	    
	    JavaRDD<Point> localConvexHullPoints = inputPoints.mapPartitions(new computeLocalConvexHull());
	    JavaRDD<List<Point>> localPointsList = localConvexHullPoints.glom();
	    
	    List<Point> convexHullPoints = localPointsList.reduce(new Function2<List<Point>, List<Point>, List<Point>>() 
	    {
			private static final long serialVersionUID = 1L;

			public List<Point> call(List<Point> arg0, List<Point> arg1) throws Exception 
			{
				QuickHull quickHull = new QuickHull();
				arg0.addAll(arg1);
				List<Point> globalPoints = quickHull.quickHull((ArrayList<Point>)arg0);
				return globalPoints;
			}
		});
	    
	    Collections.sort(convexHullPoints);
	    
	    JavaRDD<Point> globalConvexHullPoints = sc.parallelize(convexHullPoints);
	    
	    JavaRDD<Point> outputConvexHullPoints = globalConvexHullPoints.coalesce(1);
	    outputConvexHullPoints.saveAsTextFile(outputFile);
	    
	    sc.close();
	
}
	
}