package ed.inf.adbs.lightdb.operators;

public abstract class Operator {

    protected int index = 0;

    public Operator() {}

    public abstract int[] getNextTuple();

    public void reset(){ index = 0; }
}
