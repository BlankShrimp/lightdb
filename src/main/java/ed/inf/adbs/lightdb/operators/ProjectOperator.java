package ed.inf.adbs.lightdb.operators;

import com.sun.deploy.util.ArrayUtil;
import ed.inf.adbs.lightdb.utils.Catalog;
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
        System.out.println(columns.toString());
        System.out.println(columnArray.toString());
    }

    @Override
    public String[] getColumnInfo() {
        String[] result = new String[columns.size()+1];
        result[0] = columnArray.get(0);
        for (int i = 1; i < result.length; i++) {
            result[i] = columns.get(i-1).toString();
        }
        return result;
    }

    @Override
    public int[] getNextTuple() {
        int[] temp = childOperator.getNextTuple();
        if (temp!=null) {
            int[] result = new int[columns.size()];
            for (int i=0; i<columns.size(); i++) {
                result[i] = temp[columnArray.indexOf(columns.get(i).toString())-1];
            }
            return result;
        }
        return null;
    }

    @Override
    public void reset() {
        childOperator.reset();
    }
}
