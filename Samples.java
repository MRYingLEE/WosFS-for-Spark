/*
 * Copyright (c) 2016. All rights reserved. Ying LI (Mr.Ying.Lee@Gmail.com)
 */

/*
This is the samples program to use Wos Spark connector.

Generally all program which support HDFS can be migrated to use WOS by 2 steps:
1. To set up wos file system;
2. Change the file path from "hdfs://" to "wos://" in SparkContext.textFile(Paths).
    BTW, comma separated multi files string are supported in 1 statement, just like HDFS support.

There are some limits so far:
1. There is no directory/folder concept in Wos.
So all Wos objects are supposed in the root directory.
2. There is no filter on file name in SparkContext.textFile(Paths).
But you may filter files by MetaData at first, then call SparkContext.textFile(Paths) on filtered files.

The connector only solves WOS access problem. So that non-WOS specific problems may be met.
 For example, different file formats, encoding (Chinese GBK-GB2312), compressor (ZIP,gz,..)

Although the sample is coded in Java, WOS Connector should support Scala, R and Python also.
Only plain text file format has been tested. But other file formats should be supported also.

This sample suppose you have a policy "all" in your WOS cluster.

 */

package org.apache.spark.wos;

import com.ddn.wos.api.WosException;
import com.ddn.wos.api.WosPutStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import static java.time.LocalTime.now;


public final class Samples {
/*    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        // This statement does't work.
    }*/

    static SparkConf sparkConf = new SparkConf().setAppName("Wos Samples");
    static JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
    // The shared JavaSparkContext. So far only 1 active JavaSparkContext is allowed in 1 application,
    // which is decided by design of Spark.

    public static void main(String[] args) throws Exception {
        //sparkConf.setJars(new String[]{"../../../lib/wosjava.jar", "/lib64/wosjava.jar", "/lib/wosjava.jar"});
        // This statement does't work here. You should call this BEFORE the creation of SparkContext.

// *************** To prepare data ***********************

//Step : to get parameters
        String IP_Wos_cluster = "192.168.2.11";

        String localFile1 = "/lib64/spark-1.6.1-bin-hadoop2.6/LICENSE";  // This is a small file
        // You may change this to ANY text file in your OS!
        String localFile2 = "/lib64/spark-1.6.1-bin-hadoop2.6/CHANGES.txt"; // This is a relatively big file
        // You may change this to ANY text file in your OS!

        System.out.println("Usage: Samples <HOST_IP_OF_WOS>(optional) <small_file>(optional) <big_file>(optional)");
        System.out.println("Default: Samples " + IP_Wos_cluster + " " + localFile1 + " " + localFile2);
        System.out.println("So only 0,1 or 3 parameters are allowed.");

        if (!((args.length == 3) || (args.length == 1) || (args.length == 0))) {
            System.exit(1);
        }

        if (args.length > 0)
            IP_Wos_cluster = args[0]; //The host of WOS cluster. You may change according to your environment

        if (args.length == 3) {
            localFile1 = args[1];
            localFile2 = args[2];
        }

// Step : To set up wos file system
        org.apache.hadoop.conf.Configuration hadoopConf = sparkContext.hadoopConfiguration();
        //hadoopConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        //hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        hadoopConf.set("fs.wos.impl", org.apache.spark.wos.WosFileSystem.class.getName());
        // This statement is critical, users must call before use Wos!
        WosConnection.host = IP_Wos_cluster;  // This must be set before any WOS operations.

// Step : To prepare corresponding wos objects
        String wosFile1 = "wos://" + storeAsWosObject(localFile1);
        String wosFile2 = "wos://" + storeAsWosObject(localFile2);

// *************** API 1: wordCount_noLambda samples by SparkContext.textFile(Path)-- To create a standard HadoopRDD ***********************
/*
// Test 1: single small txt file
        //To compare results on local file and a corresponding object on WOS.
        wordCount_noLambda("Local 1st txt file", localFile1);
        wordCount_noLambda("Wos 1st txt file", wosFile1);

// Test 2: single big txt file
        //To compare results on local file and a corresponding object on WOS.
        wordCount_noLambda("Local 2nd txt file", localFile2);
        wordCount_noLambda("Wos 2nd txt file", wosFile2);
*/

//Test 3: 2 files together
        //To compare results on local file and a corresponding object on WOS.
        wordCount_noLambda("Local 2 txt files", localFile1 + "," + localFile2);
        wordCount_noLambda("Wos 2 txt files", wosFile1 + "," + wosFile2);

// *************** API 2: wosObjectMeta() -- To filter wos Object ***********************
// This API will provide Meta Access. You may filter files at first.
// Here, I am using Lambda statements, which needs Java JDK 1.8.
// You can use Java JDK 1.7 also. If so you need to write some traditional lengthy and ugly statements.

        List<String> paths = Arrays.asList(wosFile1, wosFile2);

        // Test 4: to check all meta data of a WOS object
        CheckMeta(wosFile1);
        CheckMeta(wosFile2);

        // Test 5: to filter WOS objects by assigned meta data
        String filteredWosFiles = paths.stream()
                .filter(path -> (new String(WosConnection.getMeta(path, "postfix")).equals("txt")))
                // be sure to use correct syntax. getMeta returns nullable byte[]!
                .collect(Collectors.joining(","));
        // to form the comma separated path list.

        // This is an ugly implementation. I will improve it to use parallel process of Spark.
        // such as: String filteredWosFiles= paths.stream()
        //.filter(path ->(new String(WosConnection.getMeta(path, "postfix")).equals("txt"))).asTextFile();

        System.out.println("Selected Wos Files:" + filteredWosFiles);

        if (!filteredWosFiles.equals(""))
            wordCount_Lambda("Selected Wos Files", filteredWosFiles);

//Step : to close all connections
        sparkContext.close();
        WosConnection.getSharedWosCluster().disconnect();
    }

    private static void CheckMeta(String path) {
        System.out.println("Check Meta");
        HashMap meta = WosConnection.getMetaAll(path);
        Iterator itr = meta.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry entry = (Map.Entry) itr.next();
            String key = (String) entry.getKey();
            String value = new String((byte[]) entry.getValue());
            System.out.printf("Meta key=%s, value=%s\n", key, value);
        }

/*
        String pathValue = new String(WosConnection.getMeta(path, "fake"));// for testing, no exception happened
        System.out.printf("Path Meta key=%s, value=%s\n", path, pathValue);
        */
    }

    // This function is copied from the famous Spark example, without meaningful modifications.
    // I only added some statements for testing only.
    private static void wordCount_noLambda(String testName, String path) {
        JavaRDD<String> textFile = sparkContext.textFile(path);
        JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) {
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });

        List<Tuple2<String, Integer>> output = counts.collect();
        System.out.println("****************************  " + testName + "  " + path); // Added for testing
        for (Tuple2<?, ?> tuple : output) {
            if (tuple._1().toString().startsWith("an"))// Added for testing
                System.out.println(tuple._1() + ": " + tuple._2());
        }
        System.out.println("****************************  " + testName + "  " + path);// Added for testing
    }

    // This function has the same function as wordCount_noLambda, but in lambda syntax, which needs JDK 1.8!
    private static void wordCount_Lambda(String testName, String path) {
        JavaRDD<String> lines = sparkContext.textFile(path);
        JavaRDD<String> words =
                lines.flatMap(line -> Arrays.asList(line.split(" ")));
        JavaPairRDD<String, Integer> counts =
                words.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
                        .reduceByKey((x, y) -> x + y);

        List<Tuple2<String, Integer>> output = counts.collect();

        System.out.println("****************************  " + testName + "  " + path); // Added for testing
        for (Tuple2<?, ?> tuple : output) {
            if (tuple._1().toString().startsWith("an"))// Added for testing
                System.out.println(tuple._1() + ": " + tuple._2());
        }
        System.out.println("****************************  " + testName + "  " + path);// Added for testing
    }

    // This function is to store a local file to WOS
    public static String storeAsWosObject(String filepath) {
        int OBJ_BUFFER_SIZE = 2000000;
        String policy = "all";

        byte[] data = new byte[OBJ_BUFFER_SIZE];
        try {
            try {
                File file = new File(filepath);
                long length = file.length();
                if (length == 0) {
                    // If this file is 0 bytes, skip it.  WOS does not support
                    // ingesting/restoring 0 byte files in v1.1.
                    return "";
                }

                WosPutStream puts = WosConnection.getSharedWosCluster().createPutStream(policy);
                puts.setMeta("path", filepath.getBytes());
                puts.setMeta("creation time", now().toString().getBytes());

                if (filepath.toLowerCase().endsWith(".txt"))
                    puts.setMeta("postfix", "txt".getBytes());

                InputStream istream = new BufferedInputStream(new FileInputStream(filepath));
                long remaining = length;
                long offset = 0;
                while (remaining > 0) {
                    int bytesRead = istream.read(data);
                    puts.putSpan(data, offset, bytesRead);
                    remaining -= bytesRead;
                    offset += bytesRead;
                }
                istream.close();
                String oid = puts.close();

                return oid;
            } catch (WosException e) {
                System.out.println("Caught WosException: " + e.getMessage());
            }
        } catch (IOException e) {
            System.out.println("Caught IOException: " + e.getMessage());
        }
        return "";
    }
}
