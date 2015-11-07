package operations.spatialoperations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

public class ShortestPointPair 
{
	@SuppressWarnings("serial")
	public static class getPoints implements Function<String, Point> 
	{
	    public Point call(String inputLines) 
	    {
	      String coordinates[] = inputLines.split("\\s*,\\s*");

	      Point point = new Point(Double.parseDouble(coordinates[0]),Double.parseDouble(coordinates[1]));
	      return  point;
	    }
	  } 
	
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
	    
	    SparkConf conf = new SparkConf().setAppName("operations.spatialoperations.ConvexHull").setMaster("spark://192.168.0.19:7077");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    JavaRDD<String> input = sc.textFile(inputFile);
	    JavaRDD<Point> inputPoints = input.map(new getPoints());
	    
	    List<Point> inputPointsList = inputPoints.collect();
	    
	    final Broadcast<List<Point>> inputPointBroadcast = sc.broadcast(inputPointsList);

	   
	    JavaPairRDD<Double,Tuple2<Point,Point>> distancePoints = inputPoints.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Point>,Double,Tuple2<Point,Point>>()
		{ 
			private static final long serialVersionUID = 1L;

			public Iterable<Tuple2<Double,Tuple2<Point,Point>>> call(Iterator<Point> inputPoints) throws Exception 
			{
				ArrayList<Tuple2<Double, Tuple2<Point,Point>>> localDistPoints = new ArrayList<Tuple2<Double, Tuple2<Point,Point>>>();
				List<Point> broadCastPoints = inputPointBroadcast.getValue();
				EucledianDistance eucledian = new EucledianDistance();
				while(inputPoints.hasNext())
				{
					Point inputPoint = inputPoints.next();
					for(Point broadCastPoint: broadCastPoints )
					{
						Double distance = eucledian.computeEucledianDistance(broadCastPoint, inputPoint);
						if(distance!=0)
							localDistPoints.add(new Tuple2<Double, Tuple2<Point,Point>>(distance, new Tuple2<Point,Point>(inputPoint,broadCastPoint)));
					}
				}			
				return localDistPoints;
			}});
	    
	    
	    JavaRDD<Double> Distances = distancePoints.map(new Function<Tuple2<Double,Tuple2<Point,Point>>,Double>()
		{
			private static final long serialVersionUID = 1L;

			public Double call(Tuple2<Double, Tuple2<Point, Point>> arg0) throws Exception 
			{
				return arg0._1;
			}
	
		});
	    
	    Double minDistance = Distances.reduce(new Function2<Double,Double,Double>()
		{
			private static final long serialVersionUID = 1L;

			public Double call(Double arg0, Double arg1)
					throws Exception 
			{
				return Math.min(arg0,arg1);
			}
	
		}
	    );
	    
	
	
	final Broadcast<Double> minShortestDistance = sc.broadcast(minDistance);

	JavaPairRDD<Double,Tuple2<Point,Point>> closestPairPoints = distancePoints.filter(new Function<Tuple2<Double,Tuple2<Point,Point>>,Boolean>()
			{
				private static final long serialVersionUID = 1L;
				public Boolean call(Tuple2<Double, Tuple2<Point, Point>> arg0) throws Exception 
				{
					return arg0._1.equals(minShortestDistance.getValue());
				}
		
			}
			
			);
	
	closestPairPoints.saveAsTextFile(outputFile);
}
	
}
