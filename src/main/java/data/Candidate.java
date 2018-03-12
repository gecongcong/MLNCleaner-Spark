package data;

import java.io.Serializable;

/**
 * Created by gcc on 17-9-28.
 */
public class Candidate implements Serializable {
    public String candidate = "";
    public double cost = 9999;
    public int tupleID = -1;

    public Candidate(int tupleID,String candidate,double cost){
        this.candidate = candidate;
        this.cost = cost;
        this.tupleID = tupleID;
    }
}
