package LSHFIndex;

import global.Vector100Dtype;
import btree.KeyClass;

public class Vector100DKey extends KeyClass {

  private Vector100Dtype key;

  public String toString(){
     return key.toString();
  }

  /** Class constructor
   *  @param     value   the value of the Vector 100D key to be set 
   */
  public Vector100DKey(Vector100Dtype value) 
  { 
    key=new Vector100Dtype(value.getDimension());
  }

  /** get a copy of the vector 100D key
   *  @return the reference of the copy 
   */
  public Vector100Dtype getKey() 
  {
    return new Vector100Dtype(key.getDimension());
  }

  /** set the integer key value
   */  
  public void setKey(Vector100Dtype value) 
  { 
    key=new Vector100Dtype(value.getDimension());
  }
}
