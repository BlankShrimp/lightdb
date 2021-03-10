package ed.inf.adbs.lightdb;

import java.util.*;
import java.util.logging.Logger;

import ed.inf.adbs.lightdb.operators.JoinOperator;
import ed.inf.adbs.lightdb.operators.ProjectOperator;
import ed.inf.adbs.lightdb.operators.ScanOperator;
import ed.inf.adbs.lightdb.operators.SelectOperator;
import ed.inf.adbs.lightdb.utils.Catalog;
import ed.inf.adbs.lightdb.utils.InterpreterHandler;
import ed.inf.adbs.lightdb.utils.LightExpressionDeParser;
import ed.inf.adbs.lightdb.utils.LightExpressionVisitorAdapter;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.TablesNamesFinder;

/**
 * Lightweight in-memory database system
 *
 */
public class LightDB {

	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}
		//TODO: 别忘了把所有的 Windows separater 换成 Linux 的

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		Catalog.LoadSchema("samples\\db");
		//TODO: 记得用 CCJSqlParserUtil.parse(new FileReader(filename)) 取代直接输入SQL，并把catch删了，把Handler的param

		try {
			Statement statement = CCJSqlParserUtil.parse("SELECT * FROM Sailors S,Reserves R where Reserves.G=Sailors.A");
			Select select = (Select) statement;
			PlainSelect plain = (PlainSelect) select.getSelectBody();
			TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
			System.out.println(plain.getFromItem().toString());
			List<Join> tables = plain.getJoins();
			for (Join t: tables) {
				System.out.println(t.toString());
			}
			// InterpreterHandler.interpret(statement);
		} catch (JSQLParserException e) {
			Logger logger = Logger.getGlobal();
			logger.severe(e.toString());
		}
		// Map<String, String> condition = new HashMap<>();
		// condition.put("leftOp", "Sailors.A");
		// condition.put("rightOp", "Reserves.G");
		// condition.put("op", "<=");
		// ScanOperator left = new ScanOperator("Sailors");
		// ScanOperator right = new ScanOperator("Reserves");
		// JoinOperator joinOperator = new JoinOperator(condition, left, right);
		// while (true) {
		// 	int[] temp = joinOperator.getNextTuple();
		// 	if (temp == null)
		// 		break;
		// 	else
		// 		System.out.println(Arrays.toString(temp));
		// }
	}

	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement from
	 * a file and prints it to screen; then extracts SelectBody from the query and
	 * prints it to screen.
	 */

	public static void parsingExample(String filename) {
		// try {
		// 	// Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
		// 	if (statement != null) {
		// 		ScanOperator scanOperator = new ScanOperator("samples\\db\\data\\Boats.csv");
		// 		Statement statement = CCJSqlParserUtil.parse("SELECT Boats.D,Boats.F FROM Boats where 1=1");
		// 		Select select = (Select) statement;
		// 		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		// 		PlainSelect plain = (PlainSelect) select.getSelectBody();
		// 		List<SelectItem> selectItems = plain.getSelectItems();
		// 		Expression expression = CCJSqlParserUtil.parseCondExpression(plain.getWhere().toString());
		// 		LightExpressionDeParser parser = new LightExpressionDeParser();
		// 		expression.accept(parser);
		//
		// 		List<String> list = parser.getList();//注意，这里list的长度可能是0。如果是0，那么直接对expression用Adapter处理
		// 		System.out.println(list.size());
		// 		List<Map<String, String>> expressions = new ArrayList<>();
		// 		// for (String s: list) {
		// 		// 	Expression expression1 = CCJSqlParserUtil.parseCondExpression(s);
		// 		// 	LightExpressionVisitorAdapter adapter = new LightExpressionVisitorAdapter();
		// 		// 	expression1.accept(adapter);
		// 		// 	Map<String,String> map = adapter.getMap();
		// 		// 	expressions.add(map);
		// 		// }
		// 		Expression expression1 = CCJSqlParserUtil.parseCondExpression(expression.toString());
		// 		LightExpressionVisitorAdapter adapter = new LightExpressionVisitorAdapter();
		// 		expression1.accept(adapter);
		// 		Map<String,String> map = adapter.getMap();
		// 		expressions.add(map);
		//
		// 		SelectOperator selectOperator = new SelectOperator(expressions, new ScanOperator("samples\\db\\data\\Boats.csv"));
		// 		ProjectOperator projectOperator = new ProjectOperator(selectItems, selectOperator);
		// 		int[] a = projectOperator.getNextTuple();
		// 		System.out.println(a[0]+" "+a[1]);
		// 		a = projectOperator.getNextTuple();
		// 		System.out.println(a[0]+" "+a[1]);
		// 		a = projectOperator.getNextTuple();
		// 		System.out.println(a[0]+" "+a[1]);
		// 		a = projectOperator.getNextTuple();
		// 		System.out.println(a[0]+" "+a[1]);
		// 		a = projectOperator.getNextTuple();
		// 		System.out.println(a[0]+" "+a[1]);
		// 		a = projectOperator.getNextTuple();
		// 		System.out.println(a[0]+" "+a[1]);
		// 	}
		// } catch (JSQLParserException e) {
		// 	System.err.println("Exception occurred during parsing");
		// 	e.printStackTrace();
		// }
	}

	private void test(int a) {

	}
}
