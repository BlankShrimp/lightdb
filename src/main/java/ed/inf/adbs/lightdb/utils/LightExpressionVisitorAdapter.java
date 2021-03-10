package ed.inf.adbs.lightdb.utils;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;

import java.util.HashMap;
import java.util.Map;

public class LightExpressionVisitorAdapter extends ExpressionVisitorAdapter {

    Map<String, String> map = new HashMap<>();

    @Override
    protected void visitBinaryExpression(BinaryExpression expression) {
        if (expression instanceof ComparisonOperator) {
            boolean leftType = true;
            boolean rightType = true;
            map.put("leftOp", expression.getLeftExpression().toString());
            map.put("op", expression.getStringExpression());
            map.put("rightOp", expression.getRightExpression().toString());

            try {
                Integer.parseInt(map.get("leftOp"));
                leftType = true;
            } catch (NumberFormatException e) {
                leftType = false;
            }
            try {
                Integer.parseInt(map.get("rightOp"));
                rightType = true;
            } catch (NumberFormatException e) {
                rightType = false;
            }

            if (!leftType && !rightType)
                map.put("type","ss");
            else if (leftType && !rightType)
                map.put("type","is");
            else if (!leftType)
                map.put("type","si");
            else
                map.put("type","ii");
        }
        super.visitBinaryExpression(expression);
    }

    public Map<String, String> getMap() {
        return map;
    }
}
