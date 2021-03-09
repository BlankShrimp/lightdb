package ed.inf.adbs.lightdb.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class Catalog {

    public static Map<String, Map<String, Integer>> schema = new HashMap<>();// TableName->(ColumnName->index)

    public static void LoadSchema(String path) {
        try {
            FileInputStream inputStream = new FileInputStream(path+"\\schema.txt");
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

    public static int getColumnIndex(String column) {
        String[] temp = column.split("\\.");
        return schema.get(temp[0]).get(temp[1]);
    }

    public static int[] getColumnsIndex(String columns) {
        String[] temp = columns.split(", ");
        int[] result = new int[temp.length];
        for (int i = 0; i < temp.length; i++) {
            result[i] = getColumnIndex(temp[i]);
        }
        return result;
    }
}
