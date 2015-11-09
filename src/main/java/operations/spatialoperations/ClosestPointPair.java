package operations.spatialoperations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

public class ClosestPointPair implements Serializable
{
	private static final long serialVersionUID = 1L;

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
	
	public void GeometryClosestPair(String inputFile,String outputFile)
	{
		SparkConf conf = new SparkConf().setAppName("operations.spatialoperations.ClosestPair");
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
	    
	    Tuple2<Double,Tuple2<Point,Point>> minDistancePointPair = distancePoints.reduce( new Function2<Tuple2<Double, Tuple2<Point,Point>>, Tuple2<Double, Tuple2<Point,Point>>, Tuple2<Double, Tuple2<Point,Point>>>() {
	    	
 			private static final long serialVersionUID = 1L;
 			
 			public Tuple2<Double,Tuple2<Point,Point>> call(Tuple2<Double, Tuple2<Point,Point>> arg0, Tuple2<Double, Tuple2<Point,Point>> arg1) throws Exception 
			{
				if(arg0._1<=arg1._1)
					return arg0;
				else
					return arg1;
			}
 			
    	});
	    
	List<Tuple2<Double,Tuple2<Point,Point>>> closestPairList = new ArrayList<Tuple2<Double,Tuple2<Point,Point>>>();
	closestPairList.add(minDistancePointPair);
	
	JavaRDD<Tuple2<Double,Tuple2<Point,Point>>> minDistancePointPairRDD = sc.parallelize(closestPairList);	
	
	JavaPairRDD<Point,Point> closestPairPointsFiltered = minDistancePointPairRDD.mapToPair(new PairFunction<Tuple2<Double,Tuple2<Point,Point>>,Point,Point>()
	{
	
		private static final long serialVersionUID = 1L;

		
		public Tuple2<Point,Point> call(Tuple2<Double,Tuple2<Point,Point>> arg0) throws Exception {
			return arg0._2;
		}
	}
			
	);
	
    JavaRDD<String> closestPairSorted = closestPairPointsFiltered.map(new Function<Tuple2<Point,Point>, String>()
    		{

				private static final long serialVersionUID = 1L;

				public String call(Tuple2<Point, Point> arg0) throws Exception {
					String outputPoints = "";
					if(arg0._1.getX()<arg0._2().getX())
						outputPoints = arg0._1.toString()+"\n"+arg0._2.toString();
					else if(arg0._1.getX()==arg0._2.getX())
					{
						if(arg0._1.getY()<=arg0._2.getY())
							outputPoints = arg0._1.toString()+"\n"+arg0._2.toString();
						else
							outputPoints = arg0._2.toString()+"\n"+arg0._1.toString();
					}
					else
						outputPoints = arg0._2.toString()+"\n"+arg0._1.toString();
					return outputPoints;
				}
    	
    		});

    closestPairSorted = closestPairSorted.coalesce(1);
    closestPairSorted.saveAsTextFile(outputFile);
	
	sc.close();
	}
	
	public static void main(String args[]) throws Exception
	{
		ClosestPointPair closestPointPair = new ClosestPointPair();
		
		String inputFile = args[0];
	    String outputFile = args[1];
	    
	    closestPointPair.GeometryClosestPair(inputFile, outputFile);
	    
	    /*SparkConf conf = new SparkConf().setAppName("operations.spatialoperations.ClosestPair");
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
	    
	    Tuple2<Double,Tuple2<Point,Point>> minDistancePointPair = distancePoints.reduce( new Function2<Tuple2<Double, Tuple2<Point,Point>>, Tuple2<Double, Tuple2<Point,Point>>, Tuple2<Double, Tuple2<Point,Point>>>() {
	    	
 			private static final long serialVersionUID = 1L;
 			
 			public Tuple2<Double,Tuple2<Point,Point>> call(Tuple2<Double, Tuple2<Point,Point>> arg0, Tuple2<Double, Tuple2<Point,Point>> arg1) throws Exception 
			{
				if(arg0._1<=arg1._1)
					return arg0;
				else
					return arg1;
			}
 			
    	});
	    
	List<Tuple2<Double,Tuple2<Point,Point>>> closestPairList = new ArrayList<Tuple2<Double,Tuple2<Point,Point>>>();
	closestPairList.add(minDistancePointPair);
	
	JavaRDD<Tuple2<Double,Tuple2<Point,Point>>> minDistancePointPairRDD = sc.parallelize(closestPairList);	
	
	JavaPairRDD<Point,Point> closestPairPointsFiltered = minDistancePointPairRDD.mapToPair(new PairFunction<Tuple2<Double,Tuple2<Point,Point>>,Point,Point>()
	{
	
		private static final long serialVersionUID = 1L;

		
		public Tuple2<Point,Point> call(Tuple2<Double,Tuple2<Point,Point>> arg0) throws Exception {
			return arg0._2;
		}
	}
			
	);
	
    JavaRDD<String> closestPairSorted = closestPairPointsFiltered.map(new Function<Tuple2<Point,Point>, String>()
    		{

				private static final long serialVersionUID = 1L;

				public String call(Tuple2<Point, Point> arg0) throws Exception {
					String outputPoints = "";
					if(arg0._1.getX()<arg0._2().getX())
						outputPoints = arg0._1.toString()+"\n"+arg0._2.toString();
					else if(arg0._1.getX()==arg0._2.getX())
					{
						if(arg0._1.getY()<=arg0._2.getY())
							outputPoints = arg0._1.toString()+"\n"+arg0._2.toString();
						else
							outputPoints = arg0._2.toString()+"\n"+arg0._1.toString();
					}
					else
						outputPoints = arg0._2.toString()+"\n"+arg0._1.toString();
					return outputPoints;
				}
    	
    		});

    closestPairSorted = closestPairSorted.coalesce(1);
    closestPairSorted.saveAsTextFile(outputFile);
	
	sc.close();*/
}
	
}
