package operations.spatialoperations;

import java.io.Serializable;
import java.util.Comparator;

public class Point implements Serializable, Comparable<Point>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Double x;
	private Double y;
	
	public Point(Double x, Double y)
	{
		this.x = x;
		this.y = y;
	}
	
	public Double getX() 
	{
		return x;
	}
	
	public void setX(Double x) 
	{
		this.x = x;
	}
	
	public Double getY() 
	{
		return y;
	}
	
	public void setY(Double y) 
	{
		this.y = y;
	}
	
	public String toString()
	{
		return getX() + "," + getY();
	}

	public int compareTo(Point B) {
		if(this.getX()<B.getX()){
			return -1;
		}
		else if(this.getX().equals(B.getX()))
		{
			if(this.getY()<B.getY()){
				return -1;
			}
			else if(this.getY().equals(B.getY())){
				return 0;
			}
			else{
				return 1;
			}
		}
		else{
			return 1;
		}
	}
	
	
}