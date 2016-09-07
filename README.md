# Integration: COMPSs and HDFS
----------------------

## How to Configure the Integration

First, you need to import the library ["HDFS_Integration.jar"](https://github.com/eubr-bigsea/compss-hdfs/tree/master/Examples/COMPSsHDFS_model) in your project. 

Second, you also need to export the enviroment variable which represent the path of hdfs, example: 

`export MASTER_HADOOP_URL=hdfs://localhost:9000`

## How to Run a application
 
Before execute a code, remember to start the HDFS:

`sbin/start-dfs.sh`

Then, I prefer to use the script like that to execute: 

`runcompss -m -d --classpath=$DIRECTORY/COMPSs_simple_HDFS.jar sample1.WordCount` which you need to change `$DIRECTORY` to the path of the jar. 

### Example: WordCount

The project *"Examples/COMPSsHDFS_model"* is a code example of how to make the COMPSs read data from a file HDFS. In this example is the following steps made:
* With the help of HDFS class, the master get the list of file blocks in HDFS;
* Each block is independent, and is able to inform each record belonging to him.


