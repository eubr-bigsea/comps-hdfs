# Integration: COMPSs and HDFS
----------------------

In order to COMPSs be able to access the HDFS's data, it has been developed an integration between COMPSs and HDFS that consists on a HDFS connector.

In order to use this integration in your COMPSs project, you only need to import the jar connector [(HDFS_Integration.jar)](https://github.com/eubr-bigsea/compss-HDFS/tree/master/HDFS_Integration/target)  in your COMPSs project (see an example of pom.xml in Examples/COMPSsHDFS_model). If you have a new version of hadoop it may be required to build a new version of the jar connector too.

The idea of this API is to use a file in the HDFS as a commom file. You can read the entire file from HDFS and then split it or you can read the same file in the HDFS already splitted. The second way is more elegant and the number of fragmentations of the file (number of splits) can be choosed. There are two options about the number of fragmentations: the first one is retrieve the natural number of logical blocks (it means, the same fragmentation adopted by HDFS to store the data), using the command *findALLBlocks(filename)* and the second way is to retrieve the blocks by forcing a number of split, using the command *findBlocksByRecords(filename,number)*.
 
Besides these two forms of choosing the fragmentation, in this current project, there are two ways to use this API. One that simply uses the API to handle the HDFS file like a commom file retrieving the blocks and then reading some data from this partition in runtime. The other way is a "static" way, meaning, you write the name of the files that will be read in the execution in a external file (config file), then it will be sent to the runcompss at the beggining of the execution and finally the natural logical blocks of the files in the config file will be retrieved. The difference between these two ways is that the last one can tell to the COMPSs where the blocks are stored to try to process these data in the right place and the other way it does not happen.
	

## How to Run a Application (first form)

It's recommended to export the enviroment variable which represents the path of HDFS, example: 

	$ export MASTER_HADOOP_URL=HDFS://<master_hostname>:<HDFS_port>
 
Before the execution of the code, remember to start the HDFS:

	$ $HADOOP_PATH/sbin/start-dfs.sh

Then, run your COMPSs's project as usual.

	

### Example: WordCount 1

There is an example in this repository explaining how to use the API in your code. The folder *"Examples/WordCount"* is a code example of how to make the COMPSs read data from a HDFS's file. In this example, the steps were:

* Use the HDFS class to retrieve the list of blocks;
* Each block is independent and it is able to return each record belonged to him.

```
//1º - Get the path of the HDFS:
String defaultFS = System.getenv("MASTER_HADOOP_URL"); 

//2º - Create a HDFS object:
HDFS dfs =  new HDFS(defaultFS);

//3º - Retrieve a list of all blocks of the file input:
ArrayList<Bloco> HDFS_SPLITS_LIST = dfs.findALLBlocks(fileHDFS);

//4º - Use each block retrieved like a file as usual, like the example:
for(Block blk : HDFS_SPLITS_LIST) {
	HashMap<String, Integer> partialResult = map(blk);
	result = mergeResults(partialResult, result);
}
```

After compile it (There is already a jar in the folder WordCount/target), use the command: 

	runcompss --classpath=$PWD/target/wordcount-hdfs.jar hdfs_1way.WordCount -i <file path in the HDFS>


## How to Run a Application (second form)

First you need to create a file in the machine that will submit the runcompss. The first line of the file will contain the address to the master node of HDFS, something like *HDFS://\<master_hostname>:\<HDFS_port>*, the other lines will contain the path of the files that will be read in the execution (one by line).

For instance, the file *confHDFS.txt* will contain:

	HDFS://localhost:9000
	/user/username/file1
	/user/username/file2
	

As the first form, remember to start the HDFS before the execution:

	$ $HADOOP_PATH/sbin/start-dfs.sh
	
Then, add the flag *storage_conf* in the your usual runcompss command:
	
	runcompss --storage_conf=$PWD/configHDFS.txt --classpath=$PWD/example.jar  ...

### Example: WordCount 2

In this other example, you will have the same steps:

* Retrieve the list of blocks;
* Each block is independent and it is able to return each record belonged to him.


```
//1º - Create a HDFS object (StorageItf):
public static StorageItf dfs;
	
//2º - Retrieve a list of all blocks of the file input (the id 0 
//     refers to the first file written in the config file):
ArrayList<Bloco> HDFS_SPLITS_LIST = storage.getBlocks(0);

//3º - Use each block retrieved like a file as usual:
for(Block blk : HDFS_SPLITS_LIST) {
	HashMap<String, Integer> partialResult = map(blk);
	result = mergeResults(partialResult, result);
}
```
After compile it (There is already a jar in the folder WordCount/target), use the command: 

	runcompss --storage_conf=$PWD/configHDFS.txt --classpath=$PWD/target/wordcount-hdfs.jar hdfs_2way.WordCount ...




## How to edit or rebuild the Integration

If you want to change or develop something in the HDFS's connector you could use the ["pom.xml"](https://github.com/eubr-bigsea/compss-HDFS/tree/master/HDFS_Integration/pom.xml) (Maven) provided in this repository to build a new jar: 

1. Build the project using the pom.xml.
2. You also can create a local repository using the command:
 	
	> 	 mvn install:install-file -Dfile=HDFS\_Integration.jar 
	>                             -DgroupId=HDFS\_Integration 
	>							 -DartifactId=HDFS\_Integration 
	>                             -Dversion=1.0  -Dpackaging=jar



