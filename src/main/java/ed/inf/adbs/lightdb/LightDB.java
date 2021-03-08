package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.lightdb.operators.ScanOperator;
import ed.inf.adbs.lightdb.operators.SelectOperator;
import ed.inf.adbs.lightdb.utils.LightExpressionDeParser;
import ed.inf.adbs.lightdb.utils.LightExpressionVisitorAdapter;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
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

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		parsingExample(inputFile);
	}

	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement from
	 * a file and prints it to screen; then extracts SelectBody from the query and
	 * prints it to screen.
	 */

	public static void parsingExample(String filename) {
		try {
			// Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
			Statement statement = CCJSqlParserUtil.parse("SELECT a,nn FROM Boats where 1=1 and 1=1");
			if (statement != null) {
				ScanOperator scanOperator = new ScanOperator("samples\\db\\data\\Boats.csv");
				Select select = (Select) statement;
				TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
				PlainSelect plain = (PlainSelect) select.getSelectBody();
				Expression expression = CCJSqlParserUtil.parseCondExpression(plain.getWhere().toString());
				LightExpressionDeParser parser = new LightExpressionDeParser();
				expression.accept(parser);

				// SelectOperator selectOperator = new SelectOperator("Boats", expression, new int[] {0,1});
				List<String> list = parser.getList();//注意，这里list的长度可能是0。如果是0，那么直接对expression用Adapter处理
				List<Map<String, String>> expressions = new ArrayList<>();
				for (String s: list) {
					Expression expression1 = CCJSqlParserUtil.parseCondExpression(s);
					LightExpressionVisitorAdapter adapter = new LightExpressionVisitorAdapter();
					expression1.accept(adapter);
					Map<String,String> map = adapter.getMap();
					expressions.add(map);
				}
				SelectOperator selectOperator = new SelectOperator(expressions, new ScanOperator("samples\\db\\data\\Boats.csv"));
				System.out.println(selectOperator.getNextTuple()[0]);
				System.out.println(selectOperator.getNextTuple()[0]);
				System.out.println(selectOperator.getNextTuple()[0]);
				System.out.println(selectOperator.getNextTuple()[0]);
				System.out.println(selectOperator.getNextTuple()[0]);
				System.out.println(selectOperator.getNextTuple()==null);
			}
		} catch (JSQLParserException e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}

	private void test(int a) {

	}
}
