package ru.fizteh.fivt.students.YaronskayaLiubov.StructuredDataTables;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Created by luba_yaronskaya on 15.11.14.
 */
public class MultiFileHashMap {
    public static String dbDir;
    protected static StoreableDataTableProvider provider;
    protected static StoreableDataTable currTable;
    private static HashMap<String, Command> multiFileHashMapCommands;
    public static boolean errorOccurred;

    public boolean exec(String[] args) throws IOException {
        dbDir = System.getProperty("fizteh.db.dir");
        if (dbDir == null) {
            System.err.println("database unspecified in properties");
            return false;
        }
        if (!Files.exists(Paths.get(dbDir))) {
            try {
                Files.createDirectory(Paths.get(dbDir));
            } catch (IOException e) {
                System.err.println("error creating database " + dbDir);
            }
        }
        provider = (StoreableDataTableProvider) new StoreableDataTableProviderFactory().create(dbDir);
        multiFileHashMapCommands = new HashMap<String, Command>();
        multiFileHashMapCommands.put("commit", new CommitCommand());
        multiFileHashMapCommands.put("create", new CreateCommand());
        multiFileHashMapCommands.put("drop", new DropCommand());
        multiFileHashMapCommands.put("exit", new ExitCommand());
        multiFileHashMapCommands.put("get", new GetCommand());
        multiFileHashMapCommands.put("list", new ListCommand());
        multiFileHashMapCommands.put("put", new PutCommand());
        multiFileHashMapCommands.put("remove", new RemoveCommand());
        multiFileHashMapCommands.put("rollback", new RollbackCommand());
        multiFileHashMapCommands.put("show", new ShowCommand());
        multiFileHashMapCommands.put("use", new UseCommand());

        if (args.length == 0) {
            while (true) {
                System.out.print(System.getProperty("user.dir") + "$ ");
                Scanner scanner = new Scanner(System.in);
                if (!scanner.hasNextLine()) {
                    break;
                }
                for (String s : scanner.nextLine().split(";")) {
                    String[] argv = s.trim().split("\\s+");
                    String curCommand = argv[0];
                    if (curCommand.equals("")) {
                        //System.out.print(System.getProperty("user.dir") + "$ ");
                        continue;
                    }
                    if (multiFileHashMapCommands.get(curCommand) == null) {
                        System.out.println(curCommand
                                + ": command not found");
                        errorOccurred = true;
                        continue;
                    }
                    try {
                        if (!multiFileHashMapCommands.get(curCommand).execute(
                                argv)) {
                            errorOccurred = true;
                        }
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                }
            }
        } else {
            StringBuilder joinedArgs = new StringBuilder();
            for (String s : args) {
                joinedArgs.append(s);
                joinedArgs.append(" ");
            }
            String[] argsPool = joinedArgs.toString().split(";");
            for (String s : argsPool) {
                String[] argv = s.trim().split("\\s+");
                String curCommand = argv[0];
                if (curCommand.equals("")) {
                    continue;
                }
                if (multiFileHashMapCommands.get(curCommand) == null) {
                    System.out.println(curCommand
                            + ": command not found");
                    errorOccurred = true;
                    continue;
                }
                try {
                    if (!multiFileHashMapCommands.get(curCommand).execute(
                            argv)) {
                        errorOccurred = true;
                    }
                } catch (Exception e) {
                    System.err.println("execution exception");
                    e.printStackTrace();
                    errorOccurred = true;
                }
            }
        }
        save();
        return !errorOccurred;
    }

    public static void save() {
        for (String tableName : provider.getTables().keySet()) {
            StoreableDataTable table = provider.tables.get(tableName);
            table.save();
        }
    }
}
