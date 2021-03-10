package ed.inf.adbs.lightdb.utils;

import com.sun.deploy.util.ArrayUtil;
import ed.inf.adbs.lightdb.operators.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
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
                // Parse the order of output columns and stored for later used.
                String[] columnOrder;
                if (selectItems.get(0).toString().equals("*")) {
                    TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
                    List<String> tables = tablesNamesFinder.getTableList(statement);
                    List<String> orderedColumns = new ArrayList<>();
                    for (String table: tables) {
                        orderedColumns.addAll(Arrays.asList(Catalog.getColumnsIndex(table)));
                    }
                    columnOrder = orderedColumns.toArray(new String[0]);
                } else {
                    columnOrder = new String[selectItems.size()];
                    for (int i = 0; i < columnOrder.length; i++) {
                        columnOrder[i] = selectItems.get(i).toString();
                    }
                }
                if (plain.getWhere()==null) {
                    if (plain.getJoins()==null) {
                        // Handle simply scanning condition.
                        ScanOperator scanOperator = new ScanOperator(plain.getFromItem().toString());
                        if (selectItems.get(0).toString().equals("*")) {
                            writeToFile(scanOperator, columnOrder);
                        } else {
                            // Handle scanning but with projection.
                            //TODO: 这里需要注意重命名
                            ProjectOperator projectOperator = new ProjectOperator(selectItems, scanOperator);
                            writeToFile(projectOperator, columnOrder);
                        }
                    } else {// With joins but without WHERE, inferring simply cross product of all tables.
                        List<String> joiningTables = new ArrayList<>();
                        for (Join join: plain.getJoins()) {
                            joiningTables.add(join.toString());
                        }
                        // Cascade all join operators by recursive calling
                        JoinOperator highestOperator = new JoinOperator(null,
                                new ScanOperator(plain.getFromItem().toString()),
                                new ScanOperator(joiningTables.get(0)));
                        for (int i=1; i<joiningTables.size();i++) {
                            highestOperator =  new JoinOperator(null,
                                    highestOperator,
                                    new ScanOperator(joiningTables.get(i)));
                        }
                        if (selectItems.get(0).toString().equals("*")) {
                            writeToFile(highestOperator, columnOrder);
                        } else {
                            // Handle scanning but with projection.
                            //TODO:这里需要注意不是所有where clause都是标准的列名（比如Boats.R可能会以R）出现
                            ProjectOperator projectOperator = new ProjectOperator(selectItems, highestOperator);
                            writeToFile(projectOperator, columnOrder);
                        }
                    }
                } else {
                    // If where clause exists:
                    Expression expression = CCJSqlParserUtil.parseCondExpression(plain.getWhere().toString());
                    LightExpressionDeParser parser = new LightExpressionDeParser();
                    expression.accept(parser);
                    List<String> list = parser.getList();
                    // Load expressions to a list
                    List<Map<String, String>> expressions = new ArrayList<>();
                    Expression expression1;// sub-expressions to be met
                    if (list.size()>0) {// The list size can be zero (if no "AND" detected)
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
                    // Group expressions: join or selection
                    List<Map<String, String>> joinExpressions = new ArrayList<>();
                    List<Map<String, String>> selectionExpressions = new ArrayList<>();
                    for (Map<String, String> map: expressions) {
                        if (map.get("type").equals("ss") &&
                                !map.get("leftOp").split("\\.")[0].equals(map.get("rightOp").split("\\.")[0]))
                            // If two sides are the same table then it can't be join
                            joinExpressions.add(map);
                        else if (map.get("type").equals("ff")) {
                            writeToFile(null, columnOrder);
                            return;
                        }
                        else if (!map.get("type").equals("ii"))
                            selectionExpressions.add(map);
                    }

                    Operator targetOperator;
                    if (plain.getJoins()==null) {
                        targetOperator = new SelectOperator(expressions, new ScanOperator(plain.getFromItem().toString()));
                    } else {
                        /* Because there is a case when tables appearing in FROM might not appear in WHERE,
                         * which means we should maintain another list for all tables in FROM.
                         * When a table is found in WHERE, remove it from the list, and cascade joining in the end.
                         *
                         * The list is specially designed that two tables can be glued together when tables are joined
                         * together. To achieve this, the format of String will be as "|t1|t2|....|"
                         * Another list that stores related Operators is also introduced.
                         *
                         * For instance, we have 'Select * From A,B,C where A.a=B.b and A.a=1', joiningTables and
                         *     joiningOperators as below
                         * 1. First init joiningTables with {"|A|","|B|","|C|"}
                         * 2. Then for each table, create scanOperator and store them to corresponding positions in
                         *     joiningOperators: {ScanA,ScanB,ScanC}
                         * 3. For each selection expression in WHERE, we query joiningTables detect if |table| exists,
                         *     if yes, extract it and its corresponding operator, construct new String and new operator,
                         *     attach them to the end of lists.
                         *     The above case is A.a=1:
                         *     - Find if |A| exists, if yes extract it: joiningTables = {"|B|","|C|"}
                         *     - Their corresponding operators as well: joiningOperators = {ScanB,ScanC}
                         *     - Create new objects: "|A|" and selectOperator("A.a=1", ScanA}
                         *     - Assign back to lists: {"|B|","|C|","|A"}, {ScanB,ScanC,SelectA}
                         * 4. For each join expression in WHERE do the same thing as Step3.
                         *     The above case is A.a=B.b:
                         *     - Find if |A| or |B| exists, if yes extract them: joiningTables = {"|C|"}
                         *     - Their corresponding operators as well: joiningOperators = {ScanC}
                         *     - Create new objects: "|A|B|" and JoinOperator("A.a=B.b", SelectA,ScanB}
                         *     - Assign back to lists: {"|C|", "|A|B|"}, {ScanC,JoinAB}
                         * 5. Finally, cascade all operators remained in joiningOperators
                         * 6. However, this approach would destroy the order of columns, which means we should correct
                         *     it now. This is why we stored column order at the beginning.
                         *
                         * A detailed explanation with figure will be introduced in README.md and README.pdf
                         */
                        // Step1
                        List<String> joiningTables = new ArrayList<>();
                        List<Operator> joiningOperators = new ArrayList<>();
                        // Different from simply cascading above, I added from item to the list.
                        joiningTables.add("|"+plain.getFromItem().toString()+"|");
                        for (Join join: plain.getJoins()) {
                            joiningTables.add("|"+join.toString()+"|");
                        }
                        // Step2
                        for (String table: joiningTables) {
                            joiningOperators.add(new ScanOperator(table.substring(1,table.length()-1)));
                        }
                        // Step3
                        for (Map<String, String> map: selectionExpressions) {
                            String tableName;
                            if (map.get("type").equals("si"))
                                tableName = map.get("leftOp").split("\\.")[0];
                            else
                                tableName = map.get("rightOp").split("\\.")[0];
                            int i = 0;
                            while (i < joiningTables.size()) {
                                if (joiningTables.get(i).contains("|"+tableName+"|")) break;
                                i++;
                            }
                            joiningTables.remove(i);
                            // This is a feature provided by selectOperator not used here: it can handle multiple expressions at once
                            List<Map<String, String>> temp = new ArrayList<>();
                            temp.add(map);
                            joiningTables.add("|"+tableName+"|");
                            joiningOperators.add(new SelectOperator(temp, joiningOperators.get(i)));
                            joiningOperators.remove(i);
                        }
                        // Step4
                        for (Map<String, String> map:joinExpressions) {
                            String leftTable = map.get("leftOp").split(("\\."))[0];
                            String rightTable = map.get("rightOp").split(("\\."))[0];
                            int l = 0;
                            int r = 0;
                            while (l < joiningTables.size()) {
                                if (joiningTables.get(l).contains("|"+leftTable+"|")) break;
                                l++;
                            }
                            while (r < joiningTables.size()) {
                                if (joiningTables.get(r).contains("|"+rightTable+"|")) break;
                                r++;
                            }
                            joiningTables.add(joiningTables.get(l)+joiningTables.get(r).substring(1));
                            joiningOperators.add(new JoinOperator(map, joiningOperators.get(l), joiningOperators.get(r)));
                            if (l>r) {// Determine the order of modification. Deleting smaller one first would cause problems
                                joiningTables.remove(l);
                                joiningTables.remove(r);
                                joiningOperators.remove(l);
                                joiningOperators.remove(r);
                            } else {
                                joiningTables.remove(r);
                                joiningTables.remove(l);
                                joiningOperators.remove(r);
                                joiningOperators.remove(l);
                            }
                        }
                        // Step5
                        Operator highestOperator;
                        if (joiningOperators.size() > 1) {
                            highestOperator = new JoinOperator(null,
                                    joiningOperators.get(0),
                                    joiningOperators.get(1));
                            for (int i=2; i<joiningOperators.size();i++) {
                                highestOperator =  new JoinOperator(null,
                                        highestOperator,
                                        joiningOperators.get(i));
                            }
                        } else
                            highestOperator = joiningOperators.get(0);
                        targetOperator = highestOperator;
                    }
                    if (selectItems.get(0).toString().equals("*")) {
                        writeToFile(targetOperator, columnOrder);
                    } else {
                        // Handle scanning but with projection.
                        //TODO:这里需要注意不是所有where clause都是标准的列名（比如Boats.R可能会以R）出现
                        ProjectOperator projectOperator = new ProjectOperator(selectItems, targetOperator);
                        writeToFile(projectOperator, columnOrder);
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
    private static void writeToFile(Operator operator, String[] columnOrder) {
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

            if (operator!=null) {
                int[] tuple = operator.getNextTuple();
                String[] tupleOrder = operator.getColumnInfo();
                int[] mapping = new int[tupleOrder.length];
                for (int i = 0; i < columnOrder.length; i++) {
                    for (int j = 0; j < tupleOrder.length; j++) {
                        if (tupleOrder[j].equals(columnOrder[i])) {
                            mapping[i] = j;
                            break;
                        }
                    }
                }
                BufferedWriter writer = new BufferedWriter(new FileWriter(Catalog.getWritePath()));
                while (tuple != null) {
                    StringBuilder temp = new StringBuilder();
                    for (int i: mapping)
                        temp.append(tuple[i]).append(",");
                    line = temp.substring(0, temp.length()-1)+"\n";
                    writer.write(line);
                    tuple = operator.getNextTuple();
                }
                writer.close();
            }
        } catch (IOException e) {
            Logger logger = Logger.getGlobal();
            logger.severe(e.toString());
        }
    }
}
