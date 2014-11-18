package ru.fizteh.fivt.students.YaronskayaLiubov.StructuredDataTables;

/**
 * Created by luba_yaronskaya on 18.11.14.
 */
public class Main {
    public static void main(String[] args) {
        boolean errorOccurred;
        try {
            errorOccurred = !new MultiFileHashMap().exec(args);
        } catch (Exception e) {
            System.err.println(e.toString());
            errorOccurred = true;
        }
        if (errorOccurred) {
            System.exit(1);
        } else {
            System.exit(0);
        }
    }
}
