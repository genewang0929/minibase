package global;

public class Vector100Dtype {

  private final static int MIN_VAL = -10000;
  private final static int MAX_VAL = 10000;
  private int[] dimension;

  public Vector100Dtype() {
    dimension = new int[100];
  }

  public Vector100Dtype(int[] dimension) {
    this.dimension = new int[100];
    setDimension(dimension);
  }
  public int[] getDimension() {
    return dimension;
  }
  public void setDimension(int[] dimension) {
    for (int i = 0; i < 100; i++) {
      if (dimension[i] > MAX_VAL)
        this.dimension[i] = MAX_VAL;
      else if (dimension[i] < MIN_VAL)
        this.dimension[i] = MIN_VAL;
      else
        this.dimension[i] = dimension[i];
    }
  }
}
