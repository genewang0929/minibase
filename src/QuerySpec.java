/**
 * Class to encapsulate the query specification.
 * It contains fields for the query type, query field, target vector file name,
 * threshold (distance or number of neighbors), and output fields.
 */
public class QuerySpec {

  private QueryType queryType;
  private int queryField; // Field number for the 100D-vector attribute.
  private String targetFileName; // Name of the file containing the target vector.
  private int threshold; // For Range: the distance D; for NN: the number K.
  private boolean useIndex; // Whether to use index or not.
  private int[] outputFields; // Field numbers to output.

  public QueryType getQueryType() {
    return queryType;
  }

  public void setQueryType(QueryType queryType) {
    this.queryType = queryType;
  }

  public int getQueryField() {
    return queryField;
  }

  public void setQueryField(int queryField) {
    this.queryField = queryField;
  }

  public String getTargetFileName() {
    return targetFileName;
  }

  public void setTargetFileName(String targetFileName) {
    this.targetFileName = targetFileName;
  }

  public int getThreshold() {
    return threshold;
  }

  public void setThreshold(int threshold) {
    this.threshold = threshold;
  }

  public void setUseIndex(boolean useIndex) {
    this.useIndex = useIndex;
  }

  public boolean getUseIndex() {
    return useIndex;
  }

  public int[] getOutputFields() {
    return outputFields;
  }

  public void setOutputFields(int[] outputFields) {
    this.outputFields = outputFields;
  }
}

