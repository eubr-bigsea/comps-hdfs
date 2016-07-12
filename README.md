# Integration: COMPSs and HDFS
----------------------

## How to Run a application 

First, you need to import the library *"HDFS_Integration.jar"*[COMPSsHDFS_model](https://github.com/eubr-bigsea/compss-hdfs/Examples/COMPSsHDFS_model) in your project. 
 
Before execute a code, remember to start the HDFS:

`sbin/start-dfs.sh`

And the rest is normal, I prefer to use the script like: 

`runcompss -m -d --classpath=$DIRECTORY/COMPSs_simple_HDFS.jar sample1.WordCount`

### Example: WordCount

The project "Examples/COMPSsHDFS_model" is a code example of how to make the COMPSs read data from a file HDFS. In this example is the following steps made:
* With the help of HDFS class, the master get the list of file blocks in HDFS;
* Each block is independent, and is able to inform each record belonging to him.


