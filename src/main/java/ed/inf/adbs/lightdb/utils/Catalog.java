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

    public static Map<String, String[]> schema = new HashMap<>();// TableName->(ordered ColumnName)
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
                String[] temp = new String[strings.length];
                for (int i = 0; i < strings.length; i++) {
                    temp[i]=strings[i];
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
     * Translate multiple column names to integers.
     * @param tableName String of column names.
     * @return Array of column name.
     */
    public static String[] getColumnsIndex(String tableName) {
        return schema.get(tableName);
    }

    public static String getPath(String tableName) {
        return pathToDB+"\\data\\"+tableName+".csv";
    }

    public static String getWritePath() {
        return pathToWriteFile;
    }
}
