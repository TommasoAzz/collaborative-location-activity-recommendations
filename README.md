# collaborative-location-activity-recommendations
<p>
  <img src="https://img.shields.io/badge/Scala-%202.12.15-green" alt="alternatetext">
  <img src="https://img.shields.io/badge/Spark-3.2.0-red" alt="alternatetext">
</p>

Project for the course "Scalable and Cloud Programming" of the University of Bologna, A.Y. 2021/2022.  
The project aims at implementing the grid-based clustering algorithm presented in the reference paper by exploiting the MapReduce paradigm.

## Stages
Our implementation is mainly composed by three stages:

- Stay points computation 
- Grid assignment
- Stay region computation

### Stay points computation

A **Stay Point** is defined as the aggregation of data from the GPS trajectories' dataset, namely [Geolife](https://www.microsoft.com/en-us/download/details.aspx?id=52367) following these conditions:

- Starting from a pivot point <em>p<sub>i</sub></em>, all the following <em>p<sub>j</sub></em> (*j* > *i* in the temporal line) points are aggregated when the distance between the pivot point is less than a distance threshold.
- When a <em>p<sub>j+1</sub></em> that breaks the distance threshold is found, the temporal threshold is checked, inspecting that the time span between <em>p<sub>i</sub></em> and <em>p<sub>j</sub></em> is above that same threshold.
- If both the conditions are fulfilled, the outcome is a Stay Point with its coordinates obtained by computing the mean of all points between <em>p<sub>i</sub></em> and <em>p<sub>j</sub></em>, time of arrival which is that of <em>p<sub>i</sub></em> and time of leave which is that of <em>p<sub>j</sub></em>.

For the stay points computation, the algorithm starts with the raw CSV dataset imported as a Spark SQL Dataframe, then it is converted to a `PairRDD` of pairs `(Int, DatasetPoint)`.
The `PairRDD` is partitioned for the parallel computation with a `RangePartitioner` in order to keep the points from the dataset in their initial order (which is time-ordered).
The second transformation phase is performed by the `groupByKey` action in order to obtain a `PairRDD` with the user identifiers as keys and a collection with all the instances of `DatasetPoint` for each user as values.
The computation of the stay points is executed through parallelization, spreading the users' collections across the workers.
Once results of the computations start finishing, the action `reduce` combines all of them in an iterable collection that is then parallelized as an `RDD` of instances of `StayPoint`.


### Grid assignment

Each Stay Point is assigned to a specific cell of the world grid starting by the Stay Point coordinates:

- Firstly, the reference system is shifted, drifting the origin of the axes to (0, 0) instead of (90, -180). The origin stays in the top left of the reference system.
- The cell's indices of the Stay Points are computed by dividing the shifted coordinates for the step's length and taking the integer part of the division

The algorithm starts with a `map` phase on the `RDD` of Stay Points, obtaining a new intermediate `RDD` of pairs `((Int, Int), StayPoint)`, in which the key is the position in the grid, namely (<em>grid<sub>X</sub></em>, <em>grid<sub>Y</sub></em>), and the value is the Stay Point.
The next transformation is a `groupByKey` which aggregates all those Stay Points within the same grid cell, yielding a `PairRDD` of pairs `((Int, Int), Iterable[StayPoint])`.
All the results are then mapped into `GridCell` instances.

### Stay regions computation

- The list of computed grid cells is ordered in a descending fashion for the number of stay points assigned to the each cell.
- Following the obtained order, we search for the neighbours cells of the selected grid cell, grouping only those cells which are not already assigned.
- Once the at most 9 grid cells are found, the centroid of all the stay points coordinates is computed. This will become the Stay Region's center.
- All selected grid cells are then marked as selected and will not be part of any other new Stay Region.

For the stay region computation, two partitioning methods are available. The one we advise is through our `GridCellPartitioner`, since the one with the `HashPartitioner` tends to approximate too much the total amount of Stay Regions.

## Cloud testing
To test the algorithm via **Google Cloud Platform (GCP)**, the first step is to enable in a Google Cloud Project the two services:

- Dataproc
- Cloud Storage

We suggest to install the Google Cloud SDK for CLIs in your system for using GCP.
Do so following [this guide](https://cloud.google.com/sdk/docs/install).

### Creating the bucket in Cloud Storage
All files for the project (JAR executables and CSV datasets) need to be stored in a Cloud Storage bucket.
```bash
gsutil mb -l $REGION gs://$BUCKET_NAME
```
`$REGION` and `$BUCKET_NAME` can be environment variables or you can just substitute them with the actual value.  
Regions can be found [here](https://cloud.google.com/about/locations).  
Beware the bucket name must be unique in the whole Google Cloud Platform, not only in your account.

### Provisioning a cluster in Dataproc
```bash
gcloud dataproc clusters create $CLUSTER_NAME --region $REGION --zone $ZONE --master-machine-type $MASTER_MACHINE_TYPE --num-workers $NUM_WORKERS --worker-machine-type $WORKER_MACHINE_TYPE
```
Again, you can use environment variables or substitute them with values. The meaning of these variables is the following:

- `$CLUSTER_NAME` is the name of the cluster, you may choose one.
- `$REGION` and `$ZONE`, please follow the link in the section above.
- `$MASTER_MACHINE_TYPE` and `$WORKER_MACHINE_TYPE` can be chosen and composed from [this list](https://cloud.google.com/compute/docs/machine-types).
- `$NUM_WORKERS` is the number of total workers (the master is not included in this number) the master can utilize.

### Compiling the project and uploading the JAR file to the bucket in Cloud Storage
```bash
sbt clean package
gsutil cp target/scala-2.12/clar.jar gs://$BUCKET_NAME/clar.jar
```
`$BUCKET_NAME` shall be the one used in the sections above.

### Uploading datasets to the bucket in Cloud Storage
We shall present the command for uploading the example dataset which can be found in the `data/` folder.
```bash
gsutil cp data/example.csv gs://$BUCKET_NAME/example.csv
```

### Submitting a job in Dataproc
```bash
gcloud dataproc jobs submit spark [--id $JOB_ID] [--async] --cluster=$CLUSTER_NAME --region=$REGION --jar=gs://$BUCKET_NAME/clar.jar -- "yarn" "gs://$BUCKET_NAME/$INPUT_FILE_NAME" "gs://$BUCKET_NAME" "sp=$SP_MODE" "sr=$SR_MODE" "$PARTITIONS"
```
Use `--async` if you want to send the job and not wait for the result.
The meaning of these variables is the following:

- `$CLUSTER_NAME`, `$REGION`, `$BUCKET_NAME` are those defined in the above sections.
- `$JOB_ID` may be chosen freely, it is just for identification purposes.
- `$INPUT_FILE_NAME` is the name of the dataset (e.g., `example.csv`).
- `$SP_MODE` can either be `sequential` or `parallel`.
- `$SR_MODE` can either be `gridcell` or `hash`.
- `$PARTITIONS` should be set as one wants, it is better if it is $NUM_WORKERS x #vCPUsPerWorker

### Downloading the results
```bash
gsutil cp -r gs://$BUCKET_NAME/stayPoints data/.
gsutil cp -r gs://$BUCKET_NAME/stayRegions data/.
```
Again, `$BUCKET_NAME` is that defined above.

### Visualizing the results (on your local machine)
```bash
pip3 install -r requirements.txt
python3 plot.py
```
Please consider using a virtual environment before launching the previous commands.
If you're a Windows user, or using the virtual environment, use `pip` and `python` instead of `pip3` and `python3`.

## References
Reference paper: Vincent W. Zheng, Yu Zheng, Xing Xie, Qiang Yang, [Collaborative Location and Activity Recommendations with GPS History Data](https://home.cse.ust.hk/~qyang/Docs/2010/www10_clar_2.pdf), 2010.
