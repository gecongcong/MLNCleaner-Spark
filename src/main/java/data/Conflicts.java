package data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Conflicts implements Serializable {
	
	List<ConflictTuple> tuples = new ArrayList<ConflictTuple>();

	ArrayList<Integer> tupleIDs = new ArrayList<>();
}
