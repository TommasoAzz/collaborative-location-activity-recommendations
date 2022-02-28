# collaborative-location-activity-recommendations
<p>
  <img src="https://img.shields.io/badge/Scala-%202.12.15-green" alt="alternatetext">
  <img src="https://img.shields.io/badge/Spark-3.2.0-red" alt="alternatetext">
</p>

Project for the course Scalable and Cloud Programming.
The project aims to implement the Grid cell clustering algorithm presented in the Reference paper by exploiting the map-reduce paradigm.

## Stages
The project is mainly composed by three stages; 
<ul>
<li>Stay points computation</li>
<li>Grid assignment</li>
<li>Stay region computation</li>
</ul>

## Stay points computation

The stay point is defined by the aggregation of data by the starting GPS trajectories' dataset, following these conditions:
<ul>
<li> Starting from a pivot point pi, the next pj (j>i in the temporal line) points are aggregated when and the distance between the pivot point is less than a distance threshold predefined</li>
<li>When the first pj that breaks the distance threshold comes, the temporal threshold is checked, inspecting that the time span between pi and pj-1 is above a given threshold </li>
<li>If all the previous conditions are fulfilled, the outcome is a Staypoint with as coordinates the mean of the coordinates of all the points between pi and pj; the stay point has a time of arrival and time of leave given by pi and pj's timestamps </li>
</ul>

For the stay points computation, the algorithm starts with the raw CSV dataset imported as a SQL Spark Dataframe, then it is converted as an RDD (UserName, DatasetPoint).
The RDD is partitioned for the parallel computation with a Range Partitioner in order to keep the dataset points in the initial order for the stay points computation.
The second "transformation" phase is the groupByKey used in order to obtain a PaiRDD with the users as key and an iterable collection with all the datasetpoints for each user as a value.
The computation of the stay points is executed parallelizing the execution, spreading the users' values across the workers and keeping the iterable collection as a monolithic solution.
The "action" reduce combine all the results in an iterable collection that is then parallelized as an RDD.


## Grid assignment

Every stay point is assigned to a specific cell of the world grid starting by the stay point's coordinates:
<ul>
<li> At first the reference system is shifted drifting the origin of the axes to (0,0) instead of (90, -180)</li>
<li> The cell's indices of the Stay Points are computed by dividing the shifted coordinates by the step's distance and taking the integer part of the outcome</li>
</ul>

The algorithm starts with a map phase on the stay points' RDD, obtaining a (gridCellPosition,StayPoint) value.
The next trasformation is a groupByKey which aggregates all the stay points with the same position as a RDD (Poistion, Iterable[StayPoints]).
All the results are then mapped as a GridCell Object.

## Stay regions computation

<ul>
<li> The cell not already assigned with the highest number of stay points is selected for the computation</li>
<li>The eight cells near the selected one are checked, if they are not already assigned, they become part of the current stay region computation</li>
<li>The nine cells aggregated, if the number of stay points' threshold is fulfilled become a new stay region</li>
</ul>

For the stay region computation, two partitioning methods are feasible. The GridCellPartitioner is the suggested one as the hash partitioner tends to approximate the final value of total stay regions.
The cells are then ordered with the total number of staypoints for each cell in order to execute the grid cell clustering algorithm.
The first cell that is not already assigned is considered for the stay region computation.
The nearest cells are then seeked within the current partition for a shuffling redution approach, and checked as "assigned". 
The Stay region is then created with the nearest cells found by the algorithm.

## Cloud testing



Reference paper: Vincent W. Zheng, Yu Zheng, Xing Xie, Qiang Yang, [Collaborative Location and Activity Recommendations with GPS History Data](https://home.cse.ust.hk/~qyang/Docs/2010/www10_clar_2.pdf), 2010.
