package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by prasad-pc on 5/1/17.
 */

public class Recover {
    String op;
    String myPortId;
    String key;
    String value;

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getMyPortId() {
        return myPortId;
    }

    public void setMyPortId(String myPortId) {
        this.myPortId = myPortId;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString(){
        return "My port="+myPortId+" Opeartion="+op+" Key="+key+ " Value="+value;
    }

}
