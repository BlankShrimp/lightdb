package ed.inf.adbs.lightdb.operators;

public abstract class Operator {

    protected int index = 0;

    public Operator() {}

    public abstract String[] getNextTuple();

    public void reset(){ index = 0; }
}
