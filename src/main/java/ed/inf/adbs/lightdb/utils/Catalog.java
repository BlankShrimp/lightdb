package ed.inf.adbs.lightdb.utils;

import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A util class with all static functions. This class achieves the following function:
 * 1. Maintain all files, paths and schema.
 * 2. Maintain and manage aliases.
 * 3. Answer files, paths, schema and aliases data requests.
 */
public class Catalog {

    public static Map<String, String[]> schema = new HashMap<>();// TableName->(ordered ColumnName)
    private static String pathToDB;
    private static String pathToWriteFile = "samples\\output\\output.csv";
    private static Map<String, String> aliasesToTableMap = new HashMap<>();
    private static Map<String, String> tableToAliasesMap = new HashMap<>();

    /**
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
                String[] temp = new String[strings.length-1];
                for (int i = 1; i < strings.length; i++) {
                    temp[i-1]=tableToAliasesMap.get(strings[0])+"."+strings[i];
                }
                schema.put(strings[0], temp);
            }
        } catch (Exception e) {
            Logger logger = Logger.getGlobal();
            logger.severe(e.toString());
        }
    }

    /**
     * Aliases is handled by keeping a mapping from BaseTable->Aliases, and a mapping Aliases->BaseTable as well.
     * @param plain The statement.
     */
    public static void handleAliases(PlainSelect plain) {
        // Step1: Add fromItem to aliases map
        String fromItem = plain.getFromItem().toString();
        if (fromItem.contains(" ")) {
            aliasesToTableMap.put(fromItem.split(" ")[1], fromItem.split(" ")[0]);
            tableToAliasesMap.put(fromItem.split(" ")[0], fromItem.split(" ")[1]);
        } else {
            aliasesToTableMap.put(fromItem, fromItem);
            tableToAliasesMap.put(fromItem, fromItem);
        }
        // Step2: Add joinItems to aliases map (if exist)
        List<String> joinList = new ArrayList<>();
        if (plain.getJoins()!=null) {
            List<Join> joins = plain.getJoins();
            for (Join join: joins) {
                if (join.toString().contains(" ")) {
                    aliasesToTableMap.put(join.toString().split(" ")[1], join.toString().split(" ")[0]);
                    tableToAliasesMap.put(join.toString().split(" ")[0], join.toString().split(" ")[1]);
                    joinList.add(join.toString().split(" ")[1]);
                } else {
                    aliasesToTableMap.put(join.toString(), join.toString());
                    tableToAliasesMap.put(join.toString(), join.toString());
                    joinList.add(join.toString());
                }
            }
        } // Until now, every table has a map (no matter whether it has an aliases)
    }

    /**
     * Translate multiple column names to integers.
     * @param tableName String of column names.
     * @return Array of column name.
     */
    public static String[] getColumnsIndex(String tableName) {
        return schema.get(aliasesToTableMap.get(tableName));
    }

    /**
     * Used delicately by ScanOperator that parse a table name to file path.
     * @param tableName String table name, must be aliases (if exists).
     * @return Path to given table.
     */
    public static String getPath(String tableName) {
        return pathToDB+"\\data\\"+aliasesToTableMap.get(tableName)+".csv";
    }

    /**
     * Update path to output file.
     * @param input Path.
     */
    public static void setWritePath(String input) {
        pathToWriteFile = input;
    }

    /**
     * A getter for output file.
     * @return Path to output file.
     */
    public static String getWritePath() {
        return pathToWriteFile;
    }
}
