package ed.inf.adbs.lightdb.operators;

import java.io.*;
import java.util.logging.Logger;

public class ScanOperator extends Operator{

    private FileInputStream fis;
    private BufferedReader bufferedReader;
    private String filename;

    public ScanOperator(String filename) {
        this.filename = filename;
        try {
            fis = new FileInputStream(filename);
            bufferedReader = new BufferedReader(new InputStreamReader(fis));
        } catch (FileNotFoundException e) {
            Logger logger = Logger.getGlobal();
            logger.severe(e.toString());
        }
    }

    /**
     * Emit next line of a given file. Calling this function will pull the
     * next line to the caller.
     * @return An array of String referring to one tuple.
     */
    @Override
    public String[] getNextTuple() {
        try {
            String str;
            if ((str = bufferedReader.readLine()) != null)
                return str.split(",");
        } catch (IOException e) {
            Logger logger = Logger.getGlobal();
            logger.severe(e.toString());
        }
        return null;
    }

    /**
     * Reset the pointer to the beginning of the file.
     */
    @Override
    public void reset() {
        closeFile();
        try {
            fis = new FileInputStream(filename);
            bufferedReader = new BufferedReader(new InputStreamReader(fis));
        } catch (FileNotFoundException e) {
            Logger logger = Logger.getGlobal();
            logger.severe(e.toString());
        }
    }

    /**
     * This function should be called before exiting program.
     */
    public void closeFile() {
        try {
            if (fis != null) {
                fis.close();
                bufferedReader.close();
            }
        } catch (IOException e) {
            Logger logger = Logger.getGlobal();
            logger.severe(e.toString());
        }
    }
}
