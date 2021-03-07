package ed.inf.adbs.lightdb.operators;

public abstract class Operator {

    private int index = 0;

    public Operator() {}

    public abstract void getNextTuple();

    public void reset(){
        index = 0;
    };
}
