package openkb.hive.udf;

import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class MyContains extends GenericUDF {

  ListObjectInspector listOI;
  StringObjectInspector elementOI;

  @Override
  public String getDisplayString(String[] arg0) {
    return "MyContains()"; 
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length != 2) {
      throw new UDFArgumentLengthException("MyContains() requires 2 arguments.");
    }

    if (!(args[0] instanceof ListObjectInspector) || !(args[1] instanceof StringObjectInspector)) {
      throw new UDFArgumentException("MyContains() requires a List and a String as the arguments.");
    }
    this.listOI = (ListObjectInspector) args[0];
    this.elementOI = (StringObjectInspector) args[1];
    
    if(!(listOI.getListElementObjectInspector() instanceof StringObjectInspector)) {
      throw new UDFArgumentException("The first argument must be a List of String.");
    }
    
    return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  }
  
  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    
    List<String> myList = (List<String>) this.listOI.getList(args[0].get());
    String myString = elementOI.getPrimitiveJavaObject(args[1].get());
    
    if (myList == null || myString == null) {
      return null;
    }
    
    for(String s: myList) {
      if (myString.equals(s)) return new Boolean(true);
    }
    return new Boolean(false);
  }
  
}
