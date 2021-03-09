package ed.inf.adbs.lightdb.utils;

import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class Catalog {

    public static Map<String, Map<String, Integer>> schema = new HashMap<>();// TableName->(ColumnName->index)
    private static String pathToDB;
    private static String pathToWriteFile = "samples\\output\\output.csv";

    /**
     * This class holds & manage all String name to integer index mapping.
     * @param path Path to database directory.
     */
    public static void LoadSchema(String path) {
        try {
            FileInputStream inputStream = new FileInputStream(path+"\\schema.txt");
            pathToDB = path;
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String str;
            while ((str=reader.readLine()) != null) {
                String[] strings = str.split(" ");
                Map<String, Integer> temp = new HashMap<>();
                for (int i = 1; i < strings.length; i++) {
                    temp.put(strings[i], i-1);
                }
                schema.put(strings[0], temp);
            }
        } catch (Exception e) {
            Logger logger = Logger.getGlobal();
            logger.severe(e.toString());
        }
    }

    public static void setWritePath(String input) {
        pathToWriteFile = input;
    }

    /**
     * Translate a column name to an integer. This function is primarily used in @SelectOperator.
     * @param column String of column name.
     * @return Integer of column index.
     */
    public static int getColumnIndex(String column) {
        String[] temp = column.split("\\.");
        return schema.get(temp[0]).get(temp[1]);
    }

    /**
     * Translate multiple column names to integers.
     * @param columns String of column names.
     * @return Array of column index.
     */
    public static int[] getColumnsIndex(String columns) {
        String[] temp = columns.split(", ");
        int[] result = new int[temp.length];
        for (int i = 0; i < temp.length; i++) {
            result[i] = getColumnIndex(temp[i]);
        }
        return result;
    }

    /**
     * Translate multiple column names to integers.
     * @param columns List of column names.
     * @return Array of column index.
     */
    public static int[] getColumnsIndex(List<SelectItem> columns) {
        int[] result = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            result[i] = getColumnIndex(columns.get(i).toString());
        }
        return result;
    }

    public static String getPath(String tableName) {
        return pathToDB+"\\data\\"+tableName+".csv";
    }

    public static String getWritePath() {
        return pathToWriteFile;
    }
}
