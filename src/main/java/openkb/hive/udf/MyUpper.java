package openkb.hive.udf;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;

@Description(
	name = "my_upper",
	value = "_FUNC_(str) - Converts a string to uppercase",
	extended = "Example:\n" +
	"  > SELECT my_upper(a) FROM test;\n" +
	"  ABC"
	)

public class MyUpper extends UDF {
  public String evaluate(String a) {
    if (a == null) { return null; }
    return a.toUpperCase();
  }
}
