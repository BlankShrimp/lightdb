package ed.inf.adbs.lightdb.operators;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JoinOperator extends Operator{

    private Operator leftChild;
    private Operator rightChild;
    private Map<String, String> condition;
    private int[] leftTupleBuffer;

    public JoinOperator(Map<String, String> condition, Operator leftChild, Operator rightChild) {
        this.condition = condition;
        this.leftChild = leftChild;
        this.rightChild = rightChild;
    }


    /**
     * Emit column names and order.
     * @return An array of column names.
     */
    @Override
    public String[] getColumnInfo() {
        String[] leftInfo = leftChild.getColumnInfo();
        String[] rightInfo = rightChild.getColumnInfo();
        int leftLength = leftInfo.length;
        int rightLength = rightInfo.length;
        leftInfo = Arrays.copyOf(leftInfo, leftLength+rightLength);
        System.arraycopy(rightInfo, 0, leftInfo, leftLength, rightLength);
        return leftInfo;
    }

    /**
     * Emit a tuple that joined two tuple with/without a condition.
     * The code seems lengthy but the switch clause takes a lot of space, and with/without a condition were written separately.
     * @return An array of int referring to one tuple.
     */
    @Override
    public int[] getNextTuple() {
        int[] result = null;
        List<String> leftColumns = Arrays.asList(leftChild.getColumnInfo());
        List<String> rightColumns = Arrays.asList(rightChild.getColumnInfo());
        if (condition!=null) {// If condition is null, then this is a cross product

            int leftIndex = leftColumns.indexOf(condition.get("leftOp"));
            int rightIndex = rightColumns.indexOf(condition.get("rightOp"));
            String op = condition.get("op");

            boolean match = false;
            while (!match) {
                int[] leftTuple;
                if (leftTupleBuffer != null)
                    leftTuple = leftTupleBuffer;
                else {
                    leftTuple = leftChild.getNextTuple();
                    leftTupleBuffer = leftTuple;
                }
                if (leftTuple!= null) {
                    while (!match) {
                        int[] rightTuple = rightChild.getNextTuple();
                        if (rightTuple != null) {
                            switch (op) {
                                case "=":
                                    if (leftTuple[leftIndex]==rightTuple[rightIndex]) {
                                        match = true;
                                        result = new int[leftTuple.length+rightTuple.length];
                                        System.arraycopy(leftTuple, 0, result, 0, leftTuple.length);
                                        System.arraycopy(rightTuple, 0, result, leftTuple.length, rightTuple.length);
                                    }
                                    break;
                                case "!=":
                                    if (leftTuple[leftIndex]!=rightTuple[rightIndex]) {
                                        match = true;
                                        result = new int[leftTuple.length+rightTuple.length];
                                        System.arraycopy(leftTuple, 0, result, 0, leftTuple.length);
                                        System.arraycopy(rightTuple, 0, result, leftTuple.length, rightTuple.length);
                                    }
                                    break;
                                case ">":
                                    if (leftTuple[leftIndex]>rightTuple[rightIndex]) {
                                        match = true;
                                        result = new int[leftTuple.length+rightTuple.length];
                                        System.arraycopy(leftTuple, 0, result, 0, leftTuple.length);
                                        System.arraycopy(rightTuple, 0, result, leftTuple.length, rightTuple.length);
                                    }
                                    break;
                                case "<":
                                    if (leftTuple[leftIndex]<rightTuple[rightIndex]) {
                                        match = true;
                                        result = new int[leftTuple.length+rightTuple.length];
                                        System.arraycopy(leftTuple, 0, result, 0, leftTuple.length);
                                        System.arraycopy(rightTuple, 0, result, leftTuple.length, rightTuple.length);
                                    }
                                    break;
                                case ">=":
                                    if (leftTuple[leftIndex]>=rightTuple[rightIndex]) {
                                        match = true;
                                        result = new int[leftTuple.length+rightTuple.length];
                                        System.arraycopy(leftTuple, 0, result, 0, leftTuple.length);
                                        System.arraycopy(rightTuple, 0, result, leftTuple.length, rightTuple.length);
                                    }
                                    break;
                                case "<=":
                                    if (leftTuple[leftIndex]<=rightTuple[rightIndex]) {
                                        match = true;
                                        result = new int[leftTuple.length+rightTuple.length];
                                        System.arraycopy(leftTuple, 0, result, 0, leftTuple.length);
                                        System.arraycopy(rightTuple, 0, result, leftTuple.length, rightTuple.length);
                                    }
                                    break;
                            }
                        } else {
                            rightChild.reset();
                            break;
                        }
                    }
                } else return null;
                if (!match)
                    leftTupleBuffer = null;
            }
        } else {
            boolean match = false;
            while (!match) {
                int[] leftTuple;
                if (leftTupleBuffer != null)
                    leftTuple = leftTupleBuffer;
                else {
                    leftTuple = leftChild.getNextTuple();
                    leftTupleBuffer = leftTuple;
                }
                if (leftTuple!= null) {
                    while (!match) {
                        int[] rightTuple = rightChild.getNextTuple();
                        if (rightTuple != null) {
                            match = true;
                            result = new int[leftTuple.length+rightTuple.length];
                            System.arraycopy(leftTuple, 0, result, 0, leftTuple.length);
                            System.arraycopy(rightTuple, 0, result, leftTuple.length, rightTuple.length);
                        } else {
                            rightChild.reset();
                            break;
                        }
                    }
                } else return null;
                if (!match)
                    leftTupleBuffer = null;
            }
        }
        return result;
    }

    /**
     * Reset the pointer to the beginning of the file.
     */
    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
    }
}
