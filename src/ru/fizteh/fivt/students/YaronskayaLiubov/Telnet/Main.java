package ru.fizteh.fivt.students.YaronskayaLiubov.Telnet;

/**
 * Created by luba_yaronskaya on 21.12.14.
 */
public class Main {
    public static void main(String[] args) {
        try {
            new Shell().exec(args);
        } catch (ShellRuntimeException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }
}
