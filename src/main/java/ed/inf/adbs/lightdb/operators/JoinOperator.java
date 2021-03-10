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
        this.condition = condition;//TODO:如果map.get("op")是空的话，那就说明是 cross product
        this.leftChild = leftChild;
        this.rightChild = rightChild;
    }


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

    @Override
    public int[] getNextTuple() {
        int[] result = null;
        List<String> leftColumns = Arrays.asList(leftChild.getColumnInfo());
        List<String> rightColumns = Arrays.asList(rightChild.getColumnInfo());
        if (condition!=null) {

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

    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
    }
}
