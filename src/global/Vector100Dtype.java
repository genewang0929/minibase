package global;

public class Vector100Dtype {

  private final static short MIN_VAL = -10000;
  private final static short MAX_VAL = 10000;
  private short[] dimension;

  public Vector100Dtype() {
    dimension = new short[100];
  }

  public Vector100Dtype(short[] dimension) {
    this.dimension = dimension;
  }
  public short[] getDimension() {
    return dimension;
  }
  public void setDimension(short[] dimension) {
    this.dimension = dimension;
  }
}
