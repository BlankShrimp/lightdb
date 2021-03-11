package ed.inf.adbs.lightdb.operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SortOperator extends Operator{

    private Operator childOperator;
    private List<int[]> buffer = new ArrayList<>();
    private int index = -1;

    public SortOperator(String[] columns, Operator childOperator) {
        this.childOperator = childOperator;
        int[] tuple = childOperator.getNextTuple();
        while (tuple != null) {
            buffer.add(tuple);
            tuple = childOperator.getNextTuple();
        }
        int[] columnPriority = new int[columns.length];
        String[] columnInfo = getColumnInfo();
        for (int i = 0; i < columns.length; i++) {
            for (int j = 0; j < columnInfo.length; j++) {
                if (columns[i].equals(columnInfo[j]))
                    columnPriority[i]=j;
            }
        }
        buffer.sort(new TupleComparator(columnPriority));
    }

    /**
     * Emit column names and order.
     * @return An array of column names.
     */
    @Override
    public String[] getColumnInfo() {
        return childOperator.getColumnInfo();
    }

    @Override
    public int[] getNextTuple() {
        index ++;
        if (buffer.isEmpty() || index>= buffer.size())
            return null;
        return buffer.get(index);
    }

    /**
     * Reset the pointer to the beginning of the file.
     */
    @Override
    public void reset() {
        childOperator.reset();
    }
}

class TupleComparator implements Comparator<int[]> {

    private int[] columnPriority;

    protected TupleComparator(int[] columnPriority) {
        this.columnPriority = columnPriority;
    }

    @Override
    public int compare(int[] o1, int[] o2) {
        for (int i: columnPriority) {
            if (o1[i] - o2[i] > 0)
                return 1;
            else if (o1[i] - o2[i] < 0)
                return -1;
        }
        return 0;
    }
}