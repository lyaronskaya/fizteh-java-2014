package ru.fizteh.fivt.students.YaronskayaLiubov.StructuredDataTables;

import ru.fizteh.fivt.storage.structured.Storeable;

/**
 * Created by luba_yaronskaya on 19.10.14.
 */
public class GetCommand extends Command {
    GetCommand() {
        name = "get";
        numberOfArguments = 2;
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
        Storeable row = MultiFileHashMap.currTable.get(args[1]);

        System.out.println((row == null) ? "not found" : MultiFileHashMap.provider.serialize(MultiFileHashMap.currTable, row));
        return true;
    }
}
