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

public class FarthestPointPair 
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
	    
	    SparkConf conf = new SparkConf().setAppName("operations.spatialoperations.FarthestPointPair").setMaster("spark://10.144.147.188:7077");
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
	    
	    
	    final Broadcast<List<Point>> convexHull = sc.broadcast(convexHullPoints);
	    
	    JavaRDD<Point> globalConvexHullPoints = sc.parallelize(convexHullPoints);
	    
	    JavaPairRDD<Double,Tuple2<Point,Point>> localPoints = globalConvexHullPoints.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Point>,Double,Tuple2<Point,Point>>()
		{ 
			private static final long serialVersionUID = 1L;

			public Iterable<Tuple2<Double,Tuple2<Point,Point>>> call(Iterator<Point> inputPoints) throws Exception 
			{
				ArrayList<Tuple2<Double, Tuple2<Point,Point>>> localDistPoints = new ArrayList<Tuple2<Double, Tuple2<Point,Point>>>();
				List<Point> broadCastPoints = convexHull.getValue();
				EucledianDistance eucledian = new EucledianDistance();
				while(inputPoints.hasNext())
				{
					Point inputPoint = inputPoints.next();
					for(Point broadCastPoint: broadCastPoints )
					{
						Double distance = eucledian.computeEucledianDistance(broadCastPoint, inputPoint);
						if(distance != 0)
						localDistPoints.add(new Tuple2<Double, Tuple2<Point,Point>>(distance, new Tuple2<Point,Point>(inputPoint,broadCastPoint)));
					}
				}			
				return localDistPoints;
			}});
	    
	   JavaRDD<Double> Distances = localPoints.map(new Function<Tuple2<Double,Tuple2<Point,Point>>,Double>()
		{
			private static final long serialVersionUID = 1L;

			public Double call(Tuple2<Double, Tuple2<Point, Point>> arg0) throws Exception 
			{
				return arg0._1;
			}
	
		});
	    
	    Double maxDistance = Distances.reduce(new Function2<Double,Double,Double>()
		{
			private static final long serialVersionUID = 1L;

			public Double call(Double arg0, Double arg1)
					throws Exception 
			{
				return Math.max(arg0,arg1);
			}
	
		}
);
	final Broadcast<Double> maxFarthestDistance = sc.broadcast(maxDistance);
	
	JavaPairRDD<Double,Tuple2<Point,Point>> FarthestPairPoint = localPoints.filter(new Function<Tuple2<Double,Tuple2<Point,Point>>,Boolean>()
			{
				private static final long serialVersionUID = 1L;
				public Boolean call(Tuple2<Double, Tuple2<Point, Point>> arg0) throws Exception 
				{
					return arg0._1.equals(maxFarthestDistance.getValue());

				}
		
			}
			
			);
	
	JavaPairRDD<Point,Point> farthestPairPoints = FarthestPairPoint.mapToPair(new PairFunction<Tuple2<Double,Tuple2<Point,Point>>,Point,Point>()
			{

				private static final long serialVersionUID = 1L;
				public Tuple2<Point, Point> call(Tuple2<Double, Tuple2<Point, Point>> arg0) throws Exception 
				{
					return arg0._2;
				}
		
			}
	
	);
	JavaPairRDD<Point,Point> farthestPairPointResult = farthestPairPoints.coalesce(1);
	farthestPairPointResult.saveAsTextFile(outputFile);
	sc.close();
}
	
}
