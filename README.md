# KmeansClustering
implement Kmeans clustering algorithm to find out similar data in a database which is stored on Hbase.

## Description
In the assignment, I firstly create a schema within Hbase to store the data, and then implement clustering algorithm that can cluster dataset into k groups by using the map and reduce primitives.

The dataset is from UCI machine learning repository. Basically, it is collected to analysis the  energy efficiency of building with different shapes. There are totally 768 data points, and each  of these points has 8 features and 2 responses. 
DataSet Description & Download: 

http://archive.ics.uci.edu/ml/datasets/Energy+efficiency  https://www.dropbox.com/s/9nctlvqjg3inedk/dataset.txt?dl=0 
K>means Tutorials:  https://dl.dropboxusercontent.com/u/73182465/eecs219/kmeans.pdf 

The dataset contains eight attributes (or features, denoted by X1...X8) and two responses (or outcomes, denoted by y1 and y2). The aim is to use the eight features to predict each of the two responses. 

Specifically: 
X1	Relative Compactness

X2	Surface Area 

X3	Wall Area 

X4	Roof Area 

X5	Overall Height 

X6	Orientation 

X7	Glazing Area 

X8	Glazing Area Distribution 

y1	Heating Load 

y2	Cooling Load

##Steps
1. create a  “center” table: randomly get 10 rows from “input” table.
2. disable and drop the tables “center” and  “input” in hbase. 
3. create a schema within Hbase to store the data. Use “/t” to split the txt data for each column. Use the hashcode of each row as row key. 

4. use BulkLoad API with a map class, load the txt file to the HBase Table named “input”, which was built previously.
5. use the API scan to scan the table “input”, generate 10 random numbers to get 10 center points as cluster centers and use a table named “center” to store these 10 rows.

6. Randomly pick up K rows from “input” table to store in another table, called “center”. K is an input of the program.

7. use a map reduce to implement K_means cluster algorithm. For the job of map, use the table “input” as input, and in the map, load another table “center”. Compare each column of the row we get from “input” table with all the K rows from “center” table, calculate the distances of all the columns, sum them. the row in “center” table that has the smallest distance is chosen as the map output key; The rowkey of the row from table “input”, chosen as output value.

8. Use a reduce job to cluster all the rows from “input” table to K clusters, and calculate new centers for these K clusters.
9. find an end point for the program. ( find a stable center point)

