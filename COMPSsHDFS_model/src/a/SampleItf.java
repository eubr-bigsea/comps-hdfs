package a;

import integratedtoolkit.types.annotations.Constraints ;
import integratedtoolkit.types.annotations.Method ;
import integratedtoolkit.types.annotations.Parameter ;
import integratedtoolkit.types.annotations.Parameter.Direction ;
import integratedtoolkit.types.annotations.Parameter.Type ;

import java.util.ArrayList;

public interface SampleItf {

	//@Constraints(processorCPUCount = 3)
	@Method(declaringClass = "a.SampleImpl")
	void conquister(
				   @Parameter(type = Type.OBJECT, direction = Direction.IN)  Bloco blk,
				   @Parameter(type = Type.FILE, direction = Direction.OUT) String output
				   );



	@Method(declaringClass = "a.SampleImpl")
	void sum(
			@Parameter(type = Type.FILE, direction = Direction.IN) 	String file,
			@Parameter(type = Type.FILE, direction = Direction.OUT) String output
	);


}
