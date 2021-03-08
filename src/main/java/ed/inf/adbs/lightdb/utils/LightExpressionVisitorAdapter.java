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
            map.put("leftOp", expression.getLeftExpression().toString());
            map.put("op", expression.getStringExpression());
            map.put("rightOp", expression.getRightExpression().toString());
        }
        super.visitBinaryExpression(expression);
    }

    public Map<String, String> getMap() {
        return map;
    }
}
