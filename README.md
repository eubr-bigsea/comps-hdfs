Integration COMPSs and HDFS
----------------------

### How to Run 

In this version, you need to import some hadoop libraries:
* "$HADOOP_DIRECTORY/share/hadoop/common/lib/commons-collections-3.2.2.jar" 
* "$HADOOP_DIRECTORY/share/hadoop/common/lib/hadoop-auth-2.7.2.jar" 
* "$HADOOP_DIRECTORY/share/hadoop/common/lib/htrace-core-3.1.0-incubating.jar" 
* "$HADOOP_DIRECTORY/share/hadoop/common/lib/servlet-api-2.5.jar" 
* "$HADOOP_DIRECTORY/share/hadoop/common/hadoop-common-2.7.2.jar" 
* "$HADOOP_DIRECTORY/share/hadoop/common/lib/commons-configuration-1.6.jar" 
* "$HADOOP_DIRECTORY/share/hadoop/common/lib/httpcore-4.2.5.jar" 
* "$HADOOP_DIRECTORY/share/hadoop/common/lib/apacheds-kerberos-codec-2.0.0-M15.jar" 
* "$HADOOP_DIRECTORY/share/hadoop/common/lib/slf4j-api-1.7.10.jar" 
* "$HADOOP_DIRECTORY/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar" 
* "$HADOOP_DIRECTORY/share/hadoop/hdfs/lib/protobuf-java-2.5.0.jar" 
* "$HADOOP_DIRECTORY/share/hadoop/hdfs/lib/commons-io-2.4.jar" 
* "$HADOOP_DIRECTORY/share/hadoop/hdfs/lib/guava-11.0.2.jar" 
* "$HADOOP_DIRECTORY/share/hadoop/hdfs/hadoop-hdfs-2.7.2.jar" 
* "$HADOOP_DIRECTORY/share/hadoop/hdfs/lib/commons-cli-1.2.jar" 
* "$HADOOP_DIRECTORY/share/hadoop/hdfs/lib/log4j-1.2.17.jar" 

### Example 1
The project "Examples/COMPSsHDFS_model" is a code example of how to make the COMPSs read data from a file HDFS. In this example is the following steps made:
* With the help of HDFS class, the teacher get the list of file blocks in HDFS.
* Each block is independent, and these will run paralemente.

In the sample code, the task "conqueror" will find the largest number in each section, and after this, a new COMPSs task will receive this number and add a certain amount.


