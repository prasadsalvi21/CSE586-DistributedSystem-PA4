package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by prasad-pc on 4/7/17.
 */

public class Node {
    String myPort;
    String pred;
    String succ;
    String dName;
    String h_pred;
    String h_succ;
    boolean isAlive;

    @Override
    public String toString(){
        return "Device="+dName+" MyPort2="+myPort+" Pred="+pred+ " Succ="+succ+" Is Alive="+isAlive+" Hash Pred="+h_pred+" Hash Succ="+h_succ;
    }

    public String getMyPort() {
        return myPort;
    }

    public void setMyPort(String myPort) {
        this.myPort = myPort;
    }

    public String getPred() {
        return pred;
    }

    public void setPred(String pred) {
        this.pred = pred;
    }

    public String getSucc() {
        return succ;
    }

    public void setSucc(String succ) {
        this.succ = succ;
    }

    public String getdName() {
        return dName;
    }

    public void setdName(String dName) {
        this.dName = dName;
    }

    public String getH_pred() {
        return h_pred;
    }

    public void setH_pred(String h_pred) {
        this.h_pred = h_pred;
    }

    public String getH_succ() {
        return h_succ;
    }

    public void setH_succ(String h_succ) {
        this.h_succ = h_succ;
    }

    public boolean isAlive() {
        return isAlive;
    }

    public void setAlive(boolean alive) {
        isAlive = alive;
    }
}
