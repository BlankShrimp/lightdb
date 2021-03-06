package ed.inf.adbs.lightdb.operators;

import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.Arrays;
import java.util.List;

public class ProjectOperator extends Operator{

    private Operator childOperator;
    private List<String> columnArray;
    private List<SelectItem> columns;

    public ProjectOperator(List<SelectItem> columns, Operator childOperator) {
        this.columns  =columns;
        this.childOperator = childOperator;
        this.columnArray = Arrays.asList(childOperator.getColumnInfo());
    }

    /**
     * Emit column names and order.
     * @return An array of column names.
     */
    @Override
    public String[] getColumnInfo() {
        String[] result = new String[columns.size()];
        result[0] = columnArray.get(0);
        for (int i = 0; i < result.length; i++) {
            result[i] = columns.get(i).toString();
        }
        return result;
    }

    /**
     * Emit a tuple with selected columns only.
     * @return An array of int referring to one tuple.
     */
    @Override
    public int[] getNextTuple() {
        int[] temp = childOperator.getNextTuple();
        if (temp!=null) {
            int[] result = new int[columns.size()];
            for (int i=0; i<columns.size(); i++) {
                result[i] = temp[columnArray.indexOf(columns.get(i).toString())];
            }
            return result;
        }
        return null;
    }

    /**
     * Reset the pointer to the beginning of the file.
     */
    @Override
    public void reset() {
        childOperator.reset();
    }
}
