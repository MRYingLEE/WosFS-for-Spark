# WosFS-for-Spark
WOS-FS is Hadoop File System Compatible. WOS-FS brings WOS of DDN.com to Apache Spark cosystem. 


#WOS-FS works like a read only HDFS
The only difference is to use "wos" instead of "hdfs" in the code.

Sample: the famous WordCountusing WOS as data source

initWos(sparkContext, String IP_of_WOS_Cluster);

……

JavaRDD<String> lines = sparkContext.textFile(("wos://...") );

JavaRDD<String> words =lines.flatMap(line -> Arrays.asList(line.split(" ")));

JavaPairRDD<String, Integer> counts =words.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
.reduceByKey((x, y) -> x + y);


#WOS-FS accelerate ETL performance
Filter WOS Objects by MetaData:

List<String> paths = Arrays.asList(wosFile1, …,wosFilen);

String filteredWosFiles= paths.stream()

.filter(path -> (new String(WosConnection.getMeta(path, "postfix")).equals("txt")))

.collect(Collectors.joining(","));

#Environment Requirements:
1. Java SDK 1.7+; (tested on 1.8.77)
2. Wos Java SDK (tested on 2.5.4) (Please put the SDK at /lib64, which is for testing only)
3. Spark 1.6 

#Before testing, 
1. Please make sure WOS JAVA demo works.
2. A WOS policy "all" in testing WOS cluster.
3. Please make sure Spark works.

#Testing
1. Parameters: WosTest <HOST_IP_OF_WOS>(optional) <small_file>(optional) <big_file>(optional)
You may use all kinds of TXT files to test. In testing, same TXT file will be processed as a local file and then as a WOS object. You may compare both results of the famous WordCount.

2. You may run the test in Spark-submit, such as:
./bin/spark-submit --class org.apache.spark.wos.wosTest --master local[2] WosConnector.jar 192.168.2.11 file1 file2 

The above command is for local testing. You may test on a spark cluster by changing the local[2] with Spark address, such as "spark://207.184.161.138:7077", which can be found in Spark console.

#Developing
1. You may write your code to test. The primary steps have been described in the source code of the testing program.
The primary statement is 
Rdd=SparkContext.textFile("wos://oid1, wos://oid2").
All the following process has nothing to do WOS any more. You just treat this RDD as a normal HDFS RDD.

2. All existing Hadoop file system (such as HDFS or local file) code can be migrated to use WOS by set upping WOS environment and change the path from "hdfs://" to "wos://".

3. You may develop your code on a local file system and then migrate to WosConnector.

# Please check the PDF file for more and colorful introduction.
