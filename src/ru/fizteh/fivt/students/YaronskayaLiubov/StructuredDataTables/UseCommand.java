package ru.fizteh.fivt.students.YaronskayaLiubov.StructuredDataTables;

/**
 * Created by luba_yaronskaya on 18.10.14.
 */
public class UseCommand extends Command {
    UseCommand() {
        name = "use";
        numberOfArguments = 2;
    }

    boolean execute(String[] args) throws Exception {
        if (args.length != numberOfArguments) {
            System.err.println(name + ": wrong number of arguements");
            return false;
        }

        String tableName = args[1];
        StoreableDataTable table = (StoreableDataTable) MultiFileHashMap.provider.getTable(tableName);
        if (table == null) {
            System.out.println(tableName + " not exists");
        } else {
            if (MultiFileHashMap.currTable != null) {
                int unsavedChanges = MultiFileHashMap.currTable.unsavedChangesCount();
                if (unsavedChanges > 0) {
                    throw new UnsavedChangesException(unsavedChanges + " unsaved changes");
                }
            }
            MultiFileHashMap.currTable = table;
            System.out.println("using " + tableName);
        }
        return true;
    }
}
