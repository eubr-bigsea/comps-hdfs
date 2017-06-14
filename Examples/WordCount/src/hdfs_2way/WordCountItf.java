package hdfs_2way;


import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.Parameter.Direction;
import integratedtoolkit.types.annotations.Parameter.Type;
import integratedtoolkit.types.annotations.task.Method;
import integration.Block;

import java.util.HashMap;

public interface WordCountItf {

	@Method(declaringClass = "sample1.WordCount")
	public HashMap<String, Integer> mergeResults(
			@Parameter HashMap<String, Integer> m1,
			@Parameter HashMap<String, Integer> m2
	);

	@Method(declaringClass = "sample1.WordCount")
	public HashMap<String, Integer> map(
			@Parameter(type = Type.OBJECT, direction = Direction.IN) Block blk
	);
}
