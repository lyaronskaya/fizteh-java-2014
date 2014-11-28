package ru.fizteh.fivt.students.YaronskayaLiubov.StructuredDataTables;

import ru.fizteh.fivt.storage.structured.Storeable;

import java.text.ParseException;

/**
 * Created by luba_yaronskaya on 19.10.14.
 */
public class PutCommand extends Command {
    PutCommand() {
        name = "put";
        numberOfArguments = 3;
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

        try {
            Storeable row = MultiFileHashMap.provider.deserialize(MultiFileHashMap.currTable, args[2]);
            Storeable old = MultiFileHashMap.currTable.put(args[1], row);
            if (old != null) {
                System.out.println("overwrite");
                System.out.println(MultiFileHashMap.provider.serialize(MultiFileHashMap.currTable, old));
            } else {
                System.out.println("new");
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return true;
    }
}
