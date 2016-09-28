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

# Please check the PDF file for more and colorful introduction.
