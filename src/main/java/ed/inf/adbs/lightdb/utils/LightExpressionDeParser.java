package ed.inf.adbs.lightdb.utils;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LightExpressionDeParser extends ExpressionDeParser {
    int depth = 0;
    private List<Map<String, Object>> list = new ArrayList<>();
    private List<String> shrinnkedList = new ArrayList<>();
    boolean called = false;

    @Override
    public void visit(Parenthesis parenthesis) {
        depth++;
        parenthesis.getExpression().accept(this);
        depth--;
    }

    @Override
    public void visit(AndExpression andExpression) {
        visitBinaryExpr(andExpression, "AND");
    }

    private void visitBinaryExpr(BinaryExpression binaryExpression, String operator) {
        Map<String, Object> map = new HashMap<>();
        if (!(binaryExpression.getLeftExpression() instanceof OrExpression)
                && !(binaryExpression.getLeftExpression() instanceof AndExpression)
                && !(binaryExpression.getLeftExpression() instanceof Parenthesis)) {
            getBuffer();
        }
        binaryExpression.getLeftExpression().accept(this);
        map.put("leftExpr", binaryExpression.getLeftExpression());
        map.put("op", operator);

        if (!(binaryExpression.getRightExpression() instanceof OrExpression)
                && !(binaryExpression.getRightExpression() instanceof AndExpression)
                && !(binaryExpression.getRightExpression() instanceof Parenthesis)) {
            getBuffer();
        }
        binaryExpression.getRightExpression().accept(this);
        map.put("rightExpr", binaryExpression.getRightExpression());
        map.put("op", operator);
        list.add(map);
    }

    public List<String> getList() {
        if (!called) {
            for(int i = 0; i<list.size();i++) {
                if(i>0) {
                    shrinnkedList.add(list.get(i).get("rightExpr").toString());
                } else {
                    shrinnkedList.add(list.get(i).get("leftExpr").toString());
                    shrinnkedList.add(list.get(i).get("rightExpr").toString());
                }
            }
        }
        return shrinnkedList;
    }

    public List<Map<String,Object>> getL() {
        return list;
    }
}
