package ru.fizteh.fivt.students.YaronskayaLiubov.Telnet;

import java.io.IOException;

/**
 * Created by luba_yaronskaya on 22.12.14.
 */
public class TestMain {
    public static void main(String[] args) throws IOException {
        StoreableDataTableProviderFactory factory = new StoreableDataTableProviderFactory();
        StoreableDataTableProvider provider = null;
        try {
            provider = (StoreableDataTableProvider) factory.create(System.getProperty("fizteh.db.dir"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        RemoteDataTableProvider remoteProvider = new RemoteDataTableProvider(provider);

        Shell shell = new Shell();
        shell.setProvider(remoteProvider);
        remoteProvider.setShell(shell);
        remoteProvider.start(10002);
        if (args.length == 0) {
            shell.startInteractiveMode();
        } else {
            shell.startPacketMode(args);
        }

    }
}
