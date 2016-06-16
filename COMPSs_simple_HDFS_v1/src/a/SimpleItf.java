package a;

import integratedtoolkit.types.annotations.Constraints ;
import integratedtoolkit.types.annotations.Method ;
import integratedtoolkit.types.annotations.Parameter ;
import integratedtoolkit.types.annotations.Parameter.Direction ;
import integratedtoolkit.types.annotations.Parameter.Type ;

public interface SimpleItf {
	@Constraints(processorCPUCount = 1)
	@Method(declaringClass = "a.SimpleImpl")
	void increment(@Parameter(type = Type.FILE, direction = Direction.INOUT) String counterFile,
				   @Parameter(type = Type.STRING, direction = Direction.IN) String defaultFS,
				   @Parameter(type = Type.STRING, direction = Direction.IN)  String fileHDFS,
				   @Parameter(type = Type.OBJECT, direction = Direction.IN)  String mapa,
				   @Parameter(type = Type.INT, direction = Direction.IN)  int item
				   );



}
