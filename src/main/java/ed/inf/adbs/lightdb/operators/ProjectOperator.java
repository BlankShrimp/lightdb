package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.utils.Catalog;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.List;

public class ProjectOperator extends Operator{

    private Operator childOperator;
    private int[] columnArray;

    public ProjectOperator(List<SelectItem> columns, Operator childOperator) {
        this.childOperator = childOperator;
        this.columnArray = Catalog.getColumnsIndex(columns);
    }

    @Override
    public int[] getNextTuple() {
        int[] temp = childOperator.getNextTuple();
        if (temp!=null) {
            int[] result = new int[columnArray.length];
            for (int i=0; i<columnArray.length; i++) {
                result[i] = temp[columnArray[i]];
            }
            return result;
        }
        return null;
    }
}
