package ru.fizteh.fivt.students.YaronskayaLiubov.proxy;

/**
 * Created by luba_yaronskaya on 10.11.14.
 */
public class UnsavedChangesException extends MultiFileMapRunTimeException {
    public UnsavedChangesException(String message) {
        super(message);
    }
}
