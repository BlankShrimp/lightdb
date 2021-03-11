package ed.inf.adbs.lightdb.operators;

import java.util.Arrays;

public class DuplicateEliminationOperator extends Operator{

    private int[] buffer;
    private Operator childOperator;

    public DuplicateEliminationOperator(Operator childOperator) {
        this.childOperator = childOperator;
    }

    /**
     * Emit column names and order.
     * @return An array of column names.
     */
    @Override
    public String[] getColumnInfo() {
        return childOperator.getColumnInfo();
    }

    /**
     * Emit a tuple guaranteed no duplicate.
     * The strategy is to store the previous tuple to a buffer, compare with the next tuple thanks to the fact that
     * the supplied tuple is sorted.
     * @return An array of int referring to one tuple.
     */
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

    /**
     * Reset the pointer to the beginning of the file.
     */
    @Override
    public void reset() {
        childOperator.reset();
    }
}
