package ed.inf.adbs.lightdb.operators;

public abstract class Operator {

    public Operator() {}

    public abstract String[] getColumnInfo();

    public abstract int[] getNextTuple();

    public abstract void reset();
}
