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


    private List<Map<String, String>> expressions;
    private Operator childOperator;
    private int[] functionArray;
    private String[] columnIndex;

    /**
     * It is worth mentioned that this select operator can handle multiple expressions at one call.
     * @param expressions A list of expressions to be checked.
     * @param childOperator Tuple supplier.
     */
    public SelectOperator(List<Map<String, String>> expressions, Operator childOperator){
        this.expressions = expressions;
        this.childOperator = childOperator;
        this.columnIndex = getColumnInfo();
        functionArray = new int[expressions.size()];
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
        }

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
     * Emit a tuple if it matches all criteria given.
     * @return An array of int referring to one tuple.
     */
    @Override
    public int[] getNextTuple() {
        int[] result;
        while (true) {
            int[] temp = childOperator.getNextTuple();
            if (temp != null) {
                boolean flag = true;
                for (int i=0;i<functionArray.length;i++) {
                    // Call different functions regarding to different operators.
                    switch (functionArray[i]) {
                        case EQUAL:
                            if (!distinguishEqual(expressions.get(i).get("leftOp"),
                                    expressions.get(i).get("rightOp"),
                                    expressions.get(i).get("type"), temp))
                                flag = false;
                            break;
                        case INEQUAL:
                            if (distinguishEqual( expressions.get(i).get("leftOp"),
                                    expressions.get(i).get("rightOp"),
                                    expressions.get(i).get("type"), temp))
                                flag = false;
                            break;
                        case GREATERTHAN:
                            if (!distinguishGreater( expressions.get(i).get("leftOp"),
                                    expressions.get(i).get("rightOp"),
                                    expressions.get(i).get("type"), temp))
                                flag = false;
                            break;
                        case LESSTHEN:
                            if (!distinguishLess( expressions.get(i).get("leftOp"),
                                    expressions.get(i).get("rightOp"),
                                    expressions.get(i).get("type"), temp))
                                flag = false;
                            break;
                        case GREATEREQUAL:
                            if (!distinguishGreaterEqual( expressions.get(i).get("leftOp"),
                                    expressions.get(i).get("rightOp"),
                                    expressions.get(i).get("type"), temp))
                                flag = false;
                            break;
                        case LESSEQUAL:
                            if (!distinguishLessEqual( expressions.get(i).get("leftOp"),
                                    expressions.get(i).get("rightOp"),
                                    expressions.get(i).get("type"), temp))
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

    /**
     * Reset the pointer to the beginning of the file.
     */
    @Override
    public void reset() {
        childOperator.reset();
    }

    /**
     * A distinguisher is to call different functions depending on different operators thanks to the fact that Java
     * is an old-school strong typing language such that I can't fit different types of objects in one function.
     * @param left Left operator.
     * @param right Right operator.
     * @param typeOfExpr One of '=' '!=' '>' '<' '>=' '<='.
     * @param tuple Pending tuple.
     * @return The selection result.
     */
    private boolean distinguishEqual(Object left, Object right, String typeOfExpr, int[] tuple) {
        // I wish I could use conditional operator here, but this is not Python,
        // it does not support different types of two conditions. Sad.
        switch (typeOfExpr) {
            case "ss":
                return isEqual(left.toString(), right.toString(), tuple);
            case "is":
                return isEqual(Integer.parseInt(left.toString()), right.toString(), tuple);
            case "si":
                return isEqual(left.toString(), Integer.parseInt(right.toString()), tuple);
            default:
                return isEqual(Integer.parseInt(left.toString()), Integer.parseInt(right.toString()), tuple);
        }
    }

    private boolean isEqual(String left, int right, int[] tuple) {
        int index = 0;
        while (index < columnIndex.length) {
            if ((columnIndex[index]).equals(left))
                break;
            index++;
        }
        return tuple[index] == right;
    }

    private boolean isEqual(int left, String right, int[] tuple) {
        int index = 0;
        while (index < columnIndex.length) {
            if ((columnIndex[index]).equals(right))
                break;
            index++;
        }
        return tuple[index] == left;
    }

    private boolean isEqual(int left, int right, int[] tuple) {
        return left == right;
    }

    private boolean isEqual(String left, String right, int[] tuple) {
        int indexLeft = 0;
        while (indexLeft < columnIndex.length) {
            if ((columnIndex[indexLeft]).equals(left))
                break;
            indexLeft++;
        }
        int indexRight = 0;
        while (indexRight < columnIndex.length) {
            if ((columnIndex[indexRight]).equals(right))
                break;
            indexRight++;
        }
        return tuple[indexLeft] == tuple[indexRight];
    }


    private boolean distinguishGreater(Object left, Object right, String typeOfExpr, int[] tuple) {
        switch (typeOfExpr) {
            case "ss":
                return isGreater(left.toString(), right.toString(), tuple);
            case "is":
                return isGreater(Integer.parseInt(left.toString()), right.toString(), tuple);
            case "si":
                return isGreater(left.toString(), Integer.parseInt(right.toString()), tuple);
            default:
                return isGreater(Integer.parseInt(left.toString()), Integer.parseInt(right.toString()), tuple);
        }
    }

    private boolean isGreater(String left, int right, int[] tuple) {
        int index = 0;
        while (index < columnIndex.length) {
            if ((columnIndex[index]).equals(left))
                break;
            index++;
        }
        return tuple[index] > right;
    }

    private boolean isGreater(int left, String right, int[] tuple) {
        int index = 0;
        while (index < columnIndex.length) {
            if ((columnIndex[index]).equals(right))
                break;
            index++;
        }
        return left > tuple[index];
    }

    private boolean isGreater(int left, int right, int[] tuple) {
        return left > right;
    }

    private boolean isGreater(String left, String right, int[] tuple) {
        int indexLeft = 0;
        while (indexLeft < columnIndex.length) {
            if ((columnIndex[indexLeft]).equals(left))
                break;
            indexLeft++;
        }
        int indexRight = 0;
        while (indexRight < columnIndex.length) {
            if ((columnIndex[indexRight]).equals(right))
                break;
            indexRight++;
        }
        return tuple[indexLeft] > tuple[indexRight];
    }


    private boolean distinguishGreaterEqual(Object left, Object right, String typeOfExpr, int[] tuple) {
        switch (typeOfExpr) {
            case "ss":
                return isGreaterEqual(left.toString(), right.toString(), tuple);
            case "is":
                return isGreaterEqual(Integer.parseInt(left.toString()), right.toString(), tuple);
            case "si":
                return isGreaterEqual(left.toString(), Integer.parseInt(right.toString()), tuple);
            default:
                return isGreaterEqual(Integer.parseInt(left.toString()), Integer.parseInt(right.toString()), tuple);
        }
    }

    private boolean isGreaterEqual(String left, int right, int[] tuple) {
        int index = 0;
        while (index < columnIndex.length) {
            if ((columnIndex[index]).equals(left))
                break;
            index++;
        }
        return tuple[index] >= right;
    }

    private boolean isGreaterEqual(int left, String right, int[] tuple) {
        int index = 0;
        while (index < columnIndex.length) {
            if ((columnIndex[index]).equals(right))
                break;
            index++;
        }
        return left >= tuple[index];
    }

    private boolean isGreaterEqual(int left, int right, int[] tuple) {
        return left >= right;
    }

    private boolean isGreaterEqual(String left, String right, int[] tuple) {
        int indexLeft = 0;
        while (indexLeft < columnIndex.length) {
            if ((columnIndex[indexLeft]).equals(left))
                break;
            indexLeft++;
        }
        int indexRight = 0;
        while (indexRight < columnIndex.length) {
            if ((columnIndex[indexRight]).equals(right))
                break;
            indexRight++;
        }
        return tuple[indexLeft] >= tuple[indexRight];
    }


    private boolean distinguishLess(Object left, Object right, String typeOfExpr, int[] tuple) {
        switch (typeOfExpr) {
            case "ss":
                return isLess(left.toString(), right.toString(), tuple);
            case "is":
                return isLess(Integer.parseInt(left.toString()), right.toString(), tuple);
            case "si":
                return isLess(left.toString(), Integer.parseInt(right.toString()), tuple);
            default:
                return isLess(Integer.parseInt(left.toString()), Integer.parseInt(right.toString()), tuple);
        }
    }

    private boolean isLess(String left, int right, int[] tuple) {
        int index = 0;
        while (index < columnIndex.length) {
            if ((columnIndex[index]).equals(left))
                break;
            index++;
        }
        return tuple[index] < right;
    }

    private boolean isLess(int left, String right, int[] tuple) {
        int index = 0;
        while (index < columnIndex.length) {
            if ((columnIndex[index]).equals(right))
                break;
            index++;
        }
        return left < tuple[index];
    }

    private boolean isLess(int left, int right, int[] tuple) {
        return left < right;
    }

    private boolean isLess(String left, String right, int[] tuple) {
        int indexLeft = 0;
        while (indexLeft < columnIndex.length) {
            if ((columnIndex[indexLeft]).equals(left))
                break;
            indexLeft++;
        }
        int indexRight = 0;
        while (indexRight < columnIndex.length) {
            if ((columnIndex[indexRight]).equals(right))
                break;
            indexRight++;
        }
        return tuple[indexLeft] < tuple[indexRight];
    }


    private boolean distinguishLessEqual(Object left, Object right, String typeOfExpr, int[] tuple) {
        switch (typeOfExpr) {
            case "ss":
                return isLessEqual(left.toString(), right.toString(), tuple);
            case "is":
                return isLessEqual(Integer.parseInt(left.toString()), right.toString(), tuple);
            case "si":
                return isLessEqual(left.toString(), Integer.parseInt(right.toString()), tuple);
            default:
                return isLessEqual(Integer.parseInt(left.toString()), Integer.parseInt(right.toString()), tuple);
        }
    }

    private boolean isLessEqual(String left, int right, int[] tuple) {
        int index = 0;
        while (index < columnIndex.length) {
            if ((columnIndex[index]).equals(left))
                break;
            index++;
        }
        return tuple[index] <= right;
    }

    private boolean isLessEqual(int left, String right, int[] tuple) {
        int index = 0;
        while (index < columnIndex.length) {
            if ((columnIndex[index]).equals(right))
                break;
            index++;
        }
        return left <= tuple[index];
    }

    private boolean isLessEqual(int left, int right, int[] tuple) {
        return left <= right;
    }

    private boolean isLessEqual(String left, String right, int[] tuple) {
        int indexLeft = 0;
        while (indexLeft < columnIndex.length) {
            if ((columnIndex[indexLeft]).equals(left))
                break;
            indexLeft++;
        }
        int indexRight = 0;
        while (indexRight < columnIndex.length) {
            if ((columnIndex[indexRight]).equals(right))
                break;
            indexRight++;
        }
        return tuple[indexLeft] <= tuple[indexRight];
    }
}
