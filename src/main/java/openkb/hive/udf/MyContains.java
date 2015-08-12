package openkb.hive.udf;

import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class MyContains extends GenericUDF {

  ListObjectInspector listOI;
  StringObjectInspector elementOI;
  private BooleanWritable result;

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
    
    result = new BooleanWritable(false);
    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }
  
  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    
    result.set(false);

    Object myList = args[0].get();
    Object myString = args[1].get();
    //System.out.println("DEBUG: myList's type is: " + myList.getClass().getName());
    //System.out.println("DEBUG: myList's 1st element's type is: " + listOI.getListElement(myList, 0).getClass().getName());
    //System.out.println("DEBUG: myString's type is: " + myString.getClass().getName());

    int arrayLength = listOI.getListLength(myList);

    if (myList == null || myString == null) {
      return result;
    }
    
    for (int i=0; i<arrayLength; ++i) {
      Object listElement = listOI.getListElement(myList, i);
      if (listElement != null) {
        if (listElement.toString().equals(myString.toString())) {
          result.set(true);
          break;
        }
      }
    }

    return result;
  }
  
}
