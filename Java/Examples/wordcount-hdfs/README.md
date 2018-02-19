
# WordCount using HDFS API

Name: WordCount

Contact Person: lucasmsp@dcc.ufmg.br

Platform: COMPSs

Language: Java



##	First Form to use the HDFS API in COMPSs

Parameters: 

* -i \<filename> : file stored in the HDFS
* -list : optional, to print the result


### Example of how to submit:


```$ runcompss -m --log_level=info --classpath=target/wordcount-hdfs.jar hdfs_1way.WordCount -i file-example.txt -list```


##	Second Form to use the HDFS API in COMPSs

Parameters: 

* -list  		  : optional, to print the result 


### Example of how to submit:

```$ runcompss -m --log_level=info --classpath=target/wordcount-hdfs.jar --storage_conf=$PWD/configHDFS.txt  hdfs_2way.WordCount -list```


