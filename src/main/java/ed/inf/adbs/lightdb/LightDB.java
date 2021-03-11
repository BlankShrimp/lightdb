package ed.inf.adbs.lightdb;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;
import java.util.logging.Logger;

import com.sun.org.apache.xpath.internal.operations.Or;
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


		try {
			Catalog.setWritePath(outputFile);
			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
			// Statement statement = CCJSqlParserUtil.parse("SELECT * FROM Reserves R,Sailors S where R.G!=S.A Order by R.H, R.G");
			InterpreterHandler.interpret(statement, databaseDir);
		} catch (JSQLParserException | FileNotFoundException e) {
			Logger logger = Logger.getGlobal();
			logger.severe(e.toString());
		}
	}
}
