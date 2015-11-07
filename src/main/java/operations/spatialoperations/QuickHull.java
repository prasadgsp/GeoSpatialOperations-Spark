package operations.spatialoperations;

import java.util.ArrayList;
 
public class QuickHull
{
    @SuppressWarnings("unchecked")
	public ArrayList<Point> quickHull(ArrayList<Point> points)
    {
        ArrayList<Point> convexHull = new ArrayList<Point>();
        if (points.size() < 3)
            return (ArrayList<Point>)points.clone();
 
        int minPoint = -1, maxPoint = -1;
        Double minX = Double.MAX_VALUE;
        Double maxX = Double.MIN_VALUE;
        
        for (int i = 0; i < points.size(); i++)
        {
            if (points.get(i).getX() < minX)
            {
                minX = points.get(i).getX();
                minPoint = i;
            }
            if (points.get(i).getX() > maxX)
            {
                maxX = points.get(i).getX();
                maxPoint = i;
            }
        }
        Point A = points.get(minPoint);
        Point B = points.get(maxPoint);
        convexHull.add(A);
        convexHull.add(B);
        points.remove(A);
        points.remove(B);
 
        ArrayList<Point> leftSet = new ArrayList<Point>();
        ArrayList<Point> rightSet = new ArrayList<Point>();
 
        for (int i = 0; i < points.size(); i++)
        {
            Point p = points.get(i);
            if (pointLocation(A, B, p) == -1)
                leftSet.add(p);
            else if (pointLocation(A, B, p) == 1)
                rightSet.add(p);
        }
        hullSet(A, B, rightSet, convexHull);
        hullSet(B, A, leftSet, convexHull);
 
        return convexHull;
    }
 
    public Double distance(Point A, Point B, Point C)
    {
        Double ABx = B.getX() - A.getX();
        Double ABy = B.getY() - A.getY();
        Double num = ABx * (A.getY() - C.getY()) - ABy * (A.getX() - C.getX());
        if (num < 0)
            num = -num;
        return num;
    }
 
    public void hullSet(Point A, Point B, ArrayList<Point> set,
            ArrayList<Point> hull)
    {
        int insertPosition = hull.indexOf(B);
        if (set.size() == 0)
            return;
        if (set.size() == 1)
        {
            Point p = set.get(0);
            set.remove(p);
            hull.add(insertPosition, p);
            return;
        }
        Double dist = Double.MIN_VALUE;
        int furthestPoint = -1;
        for (int i = 0; i < set.size(); i++)
        {
            Point p = set.get(i);
            Double distance = distance(A, B, p);
            if (distance > dist)
            {
                dist = distance;
                furthestPoint = i;
            }
        }
        Point P = set.get(furthestPoint);
        set.remove(furthestPoint);
        hull.add(insertPosition, P);
 
        // Determine who's to the left of AP
        ArrayList<Point> leftSetAP = new ArrayList<Point>();
        for (int i = 0; i < set.size(); i++)
        {
            Point M = set.get(i);
            if (pointLocation(A, P, M) == 1)
            {
                leftSetAP.add(M);
            }
        }
 
        // Determine who's to the left of PB
        ArrayList<Point> leftSetPB = new ArrayList<Point>();
        for (int i = 0; i < set.size(); i++)
        {
            Point M = set.get(i);
            if (pointLocation(P, B, M) == 1)
            {
                leftSetPB.add(M);
            }
        }
        hullSet(A, P, leftSetAP, hull);
        hullSet(P, B, leftSetPB, hull);
 
    }
 
    public int pointLocation(Point A, Point B, Point P)
    {
        Double cp1 = (B.getX() - A.getX()) * (P.getY() - A.getY()) - (B.getY() - A.getY()) * (P.getX() - A.getX());
        if (cp1 > 0)
            return 1;
        else if (cp1 == 0)
            return 0;
        else
            return -1;
    }
}
