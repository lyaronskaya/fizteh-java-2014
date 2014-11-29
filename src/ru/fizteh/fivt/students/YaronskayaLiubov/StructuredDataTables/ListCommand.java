package ru.fizteh.fivt.students.YaronskayaLiubov.StructuredDataTables;

/**
 * Created by luba_yaronskaya on 19.10.14.
 */
public class ListCommand extends Command {
    ListCommand() {
        name = "list";
        numberOfArguments = 1;
    }

    boolean execute(String[] args) {
        if (args.length != numberOfArguments) {
            System.err.println(name + ": wrong number of arguements");
            return false;
        }
        if (MultiFileHashMap.currTable == null) {
            System.err.println("no table");
            return false;
        }

        System.out.println(String.join(", ", MultiFileHashMap.currTable.list()));
        return true;
    }
}
