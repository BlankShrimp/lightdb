package ed.inf.adbs.lightdb.operators;

import java.util.List;
import java.util.Map;

public class SelectOperator extends Operator{

    private static final int EQUAL = 0;
    private static final int INEQUAL = 1;
    private static final int GREATERTHAN = 2;
    private static final int LESSTHEN = 3;
    private static final int GREATEREQUAL = 4;
    private static final int LESSEQUAL = 5;


    List<Map<String, String>> expressions;
    Operator childOperator;
    int[] functionArray;
    boolean[][] refArray;

    public SelectOperator(List<Map<String, String>> expressions, Operator childOperator){

        this.expressions = expressions;
        this.childOperator = childOperator;
        functionArray = new int[expressions.size()];
        refArray = new boolean[expressions.size()][2];
        // Construct function array and reference array. This step is to save cost from accessing var:expressions
        // times of times by keeping a record of all operators and whether a reference is an integer or a column.
        // By accessing function array and another switch clause, program will know what to do in @getNextTuple.
        for (int i = 0; i<expressions.size(); i++) {
            switch (expressions.get(i).get("op")) {
                case "=":
                    functionArray[i] = EQUAL;
                    break;
                case "!=":
                    functionArray[i] = INEQUAL;
                    break;
                case ">":
                    functionArray[i] = GREATERTHAN;
                    break;
                case "<":
                    functionArray[i] = LESSTHEN;
                    break;
                case ">=":
                    functionArray[i] = GREATEREQUAL;
                    break;
                case "<=":
                    functionArray[i] = LESSEQUAL;
                    break;
            }

            try {
                Integer.parseInt(expressions.get(i).get("leftOp"));
                refArray[i][0] = true;
            } catch (NumberFormatException e) {
                refArray[i][0] = false;
            }
            try {
                Integer.parseInt(expressions.get(i).get("rightOp"));
                refArray[i][1] = true;
            } catch (NumberFormatException e) {
                refArray[i][1] = false;
            }
        }

    }

    @Override
    public int[] getNextTuple() {
        int[] result;
        while (true) {
            int[] temp = childOperator.getNextTuple();
            if (temp != null) {
                boolean flag = true;
                for (int i=0;i<functionArray.length;i++) {
                    switch (functionArray[i]) {
                        case EQUAL:
                            if (!distinguishEqual(expressions.get(i).get("leftOp"),
                                    expressions.get(i).get("rightOp"),
                                    refArray[i][0],refArray[i][1], temp))
                                flag = false;
                            break;
                        case INEQUAL:
                            if (distinguishEqual( expressions.get(i).get("leftOp"),
                                     expressions.get(i).get("rightOp"),
                                    refArray[i][0],refArray[i][1], temp))
                                flag = false;
                            break;
                        case GREATERTHAN:
                            if (!distinguishGreater( expressions.get(i).get("leftOp"),
                                     expressions.get(i).get("rightOp"),
                                    refArray[i][0],refArray[i][1], temp))
                                flag = false;
                            break;
                        case LESSTHEN:
                            if (distinguishGreater( expressions.get(i).get("leftOp"),
                                     expressions.get(i).get("rightOp"),
                                    refArray[i][0],refArray[i][1], temp))
                                flag = false;
                            break;
                        case GREATEREQUAL:
                            if (!distinguishGreaterEqual( expressions.get(i).get("leftOp"),
                                     expressions.get(i).get("rightOp"),
                                    refArray[i][0],refArray[i][1], temp))
                                flag = false;
                            break;
                        case LESSEQUAL:
                            if (distinguishGreaterEqual( expressions.get(i).get("leftOp"),
                                     expressions.get(i).get("rightOp"),
                                    refArray[i][0],refArray[i][1], temp))
                                flag = false;
                            break;
                    }
                    if (!flag) {
                        break;
                    }
                }
                if (!flag) {
                    continue;
                }
            }
            result = temp;
            break;
        }
        return result;
    }

    @Override
    public void reset() {
        childOperator.reset();
    }

    private boolean distinguishEqual(Object left, Object right, boolean leftType, boolean rightType, int[] tuple) {
        // I wish I could use conditional operator here, but this is not Python,
        // it does not support different types of two conditions. Sad.
        if (!leftType && !rightType)
            return isEqual(left.toString(), right.toString(), tuple);
        else if (leftType && !rightType)
            return isEqual(Integer.parseInt(left.toString()), right.toString(), tuple);
        else if (!leftType)
            return isEqual(left.toString(), Integer.parseInt(right.toString()), tuple);
        else
            return isEqual(Integer.parseInt(left.toString()), Integer.parseInt(right.toString()), tuple);
    }

    private boolean isEqual(String left, int right, int[] tuple) {
        int index = Integer.parseInt(left);//TODO: 把这里的逻辑换成从Catalog获取index
        return tuple[index] == right;
    }

    private boolean isEqual(int left, String right, int[] tuple) {
        int index = Integer.parseInt(right);//TODO: 把这里的逻辑换成从Catalog获取index
        return tuple[index] == left;
    }

    private boolean isEqual(int left, int right, int[] tuple) {
        return left == right;
    }

    private boolean isEqual(String left, String right, int[] tuple) {
        int indexLeft = Integer.parseInt(left);//TODO: 把这里的逻辑换成从Catalog获取index
        int indexRight = Integer.parseInt(right);//TODO: 把这里的逻辑换成从Catalog获取index
        return tuple[indexLeft] == tuple[indexRight];
    }


    private boolean distinguishGreater(Object left, Object right, boolean leftType, boolean rightType, int[] tuple) {
        if (!leftType && !rightType)
            return isGreater(left.toString(), right.toString(), tuple);
        else if (leftType && !rightType)
            return isGreater(Integer.parseInt(left.toString()), right.toString(), tuple);
        else if (!leftType)
            return isGreater(left.toString(), Integer.parseInt(right.toString()), tuple);
        else
            return isGreater(Integer.parseInt(left.toString()), Integer.parseInt(right.toString()), tuple);
    }

    private boolean isGreater(String left, int right, int[] tuple) {
        int index = Integer.parseInt(left);//TODO: 把这里的逻辑换成从Catalog获取index
        return tuple[index] > right;
    }

    private boolean isGreater(int left, String right, int[] tuple) {
        int index = Integer.parseInt(right);//TODO: 把这里的逻辑换成从Catalog获取index
        return left > tuple[index];
    }

    private boolean isGreater(int left, int right, int[] tuple) {
        return left > right;
    }

    private boolean isGreater(String left, String right, int[] tuple) {
        int indexLeft = Integer.parseInt(left);//TODO: 把这里的逻辑换成从Catalog获取index
        int indexRight = Integer.parseInt(right);//TODO: 把这里的逻辑换成从Catalog获取index
        return tuple[indexLeft] > tuple[indexRight];
    }


    private boolean distinguishGreaterEqual(Object left, Object right, boolean leftType, boolean rightType, int[] tuple) {
        if (!leftType && !rightType)
            return isGreaterEqual(left.toString(), right.toString(), tuple);
        else if (leftType && !rightType)
            return isGreaterEqual(Integer.parseInt(left.toString()), right.toString(), tuple);
        else if (!leftType)
            return isGreaterEqual(left.toString(), Integer.parseInt(right.toString()), tuple);
        else
            return isGreaterEqual(Integer.parseInt(left.toString()), Integer.parseInt(right.toString()), tuple);
    }

    private boolean isGreaterEqual(String left, int right, int[] tuple) {
        int index = Integer.parseInt(left);//TODO: 把这里的逻辑换成从Catalog获取index
        return tuple[index] >= right;
    }

    private boolean isGreaterEqual(int left, String right, int[] tuple) {
        int index = Integer.parseInt(right);//TODO: 把这里的逻辑换成从Catalog获取index
        return left >= tuple[index];
    }

    private boolean isGreaterEqual(int left, int right, int[] tuple) {
        return left >= right;
    }

    private boolean isGreaterEqual(String left, String right, int[] tuple) {
        int indexLeft = Integer.parseInt(left);//TODO: 把这里的逻辑换成从Catalog获取index
        int indexRight = Integer.parseInt(right);//TODO: 把这里的逻辑换成从Catalog获取index
        return tuple[indexLeft] >= tuple[indexRight];
    }
}
