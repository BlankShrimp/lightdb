package ed.inf.adbs.lightdb.utils;

import com.sun.deploy.util.ArrayUtil;
import ed.inf.adbs.lightdb.operators.Operator;
import ed.inf.adbs.lightdb.operators.ProjectOperator;
import ed.inf.adbs.lightdb.operators.ScanOperator;
import ed.inf.adbs.lightdb.operators.SelectOperator;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class InterpreterHandler {

    /**
     * Main function that handles all top-level interpreting logic.
     * @param statement SQL statement.
     */
    public static void interpret(Statement statement) {
        try {
            if (statement != null) {
                Select select = (Select) statement;
                PlainSelect plain = (PlainSelect) select.getSelectBody();

                // First, get select items and where expression (if exists) from statement.
                List<SelectItem> selectItems = plain.getSelectItems();
                if (plain.getWhere()==null) {
                    // Handle simply scanning condition.
                    TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
                    ScanOperator scanOperator = new ScanOperator(Catalog.getPath(tablesNamesFinder.getTableList(statement).get(0)));
                    if (selectItems.get(0).toString().equals("*")) {
                        writeToFile(scanOperator);
                    } else {
                        // Handle scanning but with projection.
                        ProjectOperator projectOperator = new ProjectOperator(selectItems, scanOperator);
                        writeToFile(projectOperator);
                    }
                } else {
                    // If where clause exists:
                    Expression expression = CCJSqlParserUtil.parseCondExpression(plain.getWhere().toString());
                    LightExpressionDeParser parser = new LightExpressionDeParser();
                    expression.accept(parser);
                    List<String> list = parser.getList();// The list size can be zero (if no "AND" detected)
                    List<Map<String, String>> expressions = new ArrayList<>();
                    Expression expression1;// sub-expressions to be met
                    if (list.size()>0) {
                        for (String s: list) {
                            expression1 = CCJSqlParserUtil.parseCondExpression(s);
                            LightExpressionVisitorAdapter adapter = new LightExpressionVisitorAdapter();
                            expression1.accept(adapter);
                            Map<String,String> map = adapter.getMap();
                            expressions.add(map);
                        }
                    } else {
                        expression1 = CCJSqlParserUtil.parseCondExpression(expression.toString());
                        LightExpressionVisitorAdapter adapter = new LightExpressionVisitorAdapter();
                        expression1.accept(adapter);
                        Map<String,String> map = adapter.getMap();
                        expressions.add(map);
                    }
                    SelectOperator selectOperator = new SelectOperator(expressions, new ScanOperator("samples\\db\\data\\Boats.csv"));
                    if (selectItems.get(0).toString().equals("*")) {
                        writeToFile(selectOperator);
                    } else {
                        // Handle scanning but with projection.
                        ProjectOperator projectOperator = new ProjectOperator(selectItems, selectOperator);
                        writeToFile(projectOperator);
                    }
                }
            }
        } catch (JSQLParserException e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }
    }

    /**
     * Write query result to file.
     * @param operator The root Operator.
     */
    private static void writeToFile(Operator operator) {
        String line;
        try {
            // Create file if not exists
            StringBuilder dirName = new StringBuilder();
            String[] a = Catalog.getWritePath().split("\\\\");
            for (int i = 0; i < a.length-2; i++) {
                dirName.append(a[i]).append("\\");
            }
            dirName.append(a[a.length-2]);
            File dir = new File(dirName.toString());
            if (!dir.exists())
                dir.mkdirs();
            File file = new File(dir, a[a.length-1]);
            if(!file.exists())
                file.createNewFile();

            int[] tuple = operator.getNextTuple();
            BufferedWriter writer = new BufferedWriter(new FileWriter(Catalog.getWritePath()));
            while (tuple != null) {
                StringBuilder temp = new StringBuilder();
                for (int i: tuple)
                    temp.append(i).append(",");
                line = temp.substring(0, temp.length()-1)+"\n";
                writer.write(line);
                tuple = operator.getNextTuple();
            }
            writer.close();
        } catch (IOException e) {
            Logger logger = Logger.getGlobal();
            logger.severe(e.toString());
        }
    }
}
