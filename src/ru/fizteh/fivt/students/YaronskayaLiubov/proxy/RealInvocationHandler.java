package ru.fizteh.fivt.students.YaronskayaLiubov.proxy;

import java.io.Writer;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by luba_yaronskaya on 01.12.14.
 */
public class RealInvocationHandler implements InvocationHandler {
    private Writer writer;
    private Object implementation;

    public RealInvocationHandler(Writer writer, Object implementation) {
        this.writer = writer;
        this.implementation = implementation;
    }

    @Override
    public synchronized Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
JSONObject log = new JSONObject();
        log.put("timestamp", System.currentTimeMillis());
        log.put("class", implementation.getClass().getName());
        log.put("method", method.getName());
        log.put("arguments", getJSONArray(args));
        Throwable thrownException = null;
        Object returnValue = null;
        try {
            returnValue = method.invoke(implementation, args);
        } catch (InvocationTargetException e) {
            thrownException = e.getTargetException();
        }
        if (thrownException != null) {
            log.put("thrown", thrownException);
        } else {
            log.put("return value", returnValue);
        }

        writer.write(log.toString());

        return returnValue;
    }
public JSONArray getJSONArray(Object[] args) {
    JSONArray res = new JSONArray();
    for (Object val : args) {
        res.put(val);
    }
}
}
