package operations.spatialoperations;

public class EucledianDistance 
{

	public Double computeEucledianDistance(Point a, Point b)
	{
		
		Double distance = Math.sqrt(Math.pow(a.getX()- b.getX(), 2)+Math.pow(a.getY()- b.getY(), 2));
		return distance;
	}
} 
