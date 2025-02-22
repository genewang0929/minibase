package global;

public class Vector100Dtype {

  private final static int MIN_VAL = -10000;
  private final static int MAX_VAL = 10000;
  private int[] dimension;

  public Vector100Dtype() {
    dimension = new int[100];
  }

  public Vector100Dtype(int[] dimension) {
    this.dimension = dimension;
  }
  public int[] getDimension() {
    return dimension;
  }
  public void setDimension(int[] dimension) {
    this.dimension = dimension;
  }
}
