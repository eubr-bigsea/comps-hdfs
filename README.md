# Integration: COMPSs and HDFS
----------------------

In order to COMPSs be able to access the HDFS's data, it has been developed an integration COMPSs and HDFS. This integration consists on a HDFS connector.

In order to use this integration in your COMPSs project, you only need to import the jar connector [(HDFS_Integration.jar)](https://github.com/eubr-bigsea/compss-hdfs/tree/master/HDFS_Integration/target)  in your COMPSs project (see an example of pom.xml in Examples/COMPSsHDFS_model).

It's recommended to export the enviroment variable which represents the path of hdfs, example: 

	$ export MASTER_HADOOP_URL=hdfs://<master_hostname>:<hdfs_port>
	

## How to Run a Application
 
Before the execution of the code, remember to start the HDFS:

	$ $HADOOP_PATH/sbin/start-dfs.sh

Then, run your COMPSs's project as usual.

	

### Example: WordCount

There is an example in this repository how to use the API in your code. The folder *"Examples/COMPSsHDFS_model"* is a code example of how to make the COMPSs read data from a HDFS's file. In this example, the steps were:

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

After compile it, use the command: 

	runcompss --classpath=target/example-1.0.jar sample1.WordCount -i <file path in the HDFS>


## How to edit the Integration

If you want to change or develop something in the HDFS's connector you could use the ["pom.xml"](https://github.com/eubr-bigsea/compss-hdfs/tree/master/HDFS_Integration/pom.xml) (Maven) provided in this repository to build a new jar: 

1. Build the project using the pom.xml.
2. You also can create a local repository using the command:
 	
	> 	 mvn install:install-file -Dfile=HDFS\_Integration.jar 
	>                             -DgroupId=HDFS\_Integration 
	>							 -DartifactId=HDFS\_Integration 
	>                             -Dversion=1.0  -Dpackaging=jar



