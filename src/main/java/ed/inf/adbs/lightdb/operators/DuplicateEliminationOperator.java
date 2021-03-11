package ed.inf.adbs.lightdb.operators;

import java.util.Arrays;

public class DuplicateEliminationOperator extends Operator{

    private int[] buffer;
    private Operator childOperator;

    public DuplicateEliminationOperator(Operator childOperator) {
        this.childOperator = childOperator;
    }

    @Override
    public String[] getColumnInfo() {
        return childOperator.getColumnInfo();
    }

    @Override
    public int[] getNextTuple() {
        if (buffer==null) {
            buffer = childOperator.getNextTuple();
            return buffer;
        } else {
            while (true) {
                int[] nextBuffer = childOperator.getNextTuple();
                if (nextBuffer==null)
                    return null;
                if (!Arrays.equals(buffer, nextBuffer)) {
                    buffer = nextBuffer;
                    return buffer;
                }
            }
        }
    }

    @Override
    public void reset() {
        childOperator.reset();
    }
}
