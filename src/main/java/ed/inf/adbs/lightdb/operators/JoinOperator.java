package ed.inf.adbs.lightdb.operators;

import java.util.Map;

public class JoinOperator extends Operator{

    private Operator leftChild;
    private Operator rightChild;
    private Map<String, String> condition;

    public JoinOperator(Map<String, String> condition, Operator leftChild, Operator rightChild) {
        this.condition = condition;//TODO:如果map.get("op")是空的话，那就说明是 cross product
        this.leftChild = leftChild;
        this.rightChild = rightChild;
    }


    @Override
    public String[] getColumnInfo() {
        return new String[0];
    }

    @Override
    public int[] getNextTuple() {
        return new int[0];
    }

    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
    }
}
