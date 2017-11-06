package hdfs_2way;


import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Type;
import integratedtoolkit.types.annotations.task.Method;
import integration.BlockLocality;

import java.util.HashMap;

public interface WordCountItf {

	@Method(declaringClass = "hdfs_2way.WordCount")
	public HashMap<String, Integer> mergeResults(
			@Parameter HashMap<String, Integer> m1,
			@Parameter HashMap<String, Integer> m2
	);

	@Method(declaringClass = "hdfs_2way.WordCount")
	public HashMap<String, Integer> map(
			@Parameter(type = Type.OBJECT, direction = Direction.IN) BlockLocality blk
	);
}
