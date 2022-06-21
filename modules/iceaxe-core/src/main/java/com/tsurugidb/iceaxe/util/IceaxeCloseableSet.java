package com.tsurugidb.iceaxe.util;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Iceaxe Closeable set
 */
@ThreadSafe
public class IceaxeCloseableSet {

    private final Map<Closeable, ?> closeableMap = new IdentityHashMap<>();

    /**
     * add Closeable
     * 
     * @param closeable Closeable
     */
    public synchronized void add(Closeable closeable) {
        closeableMap.put(closeable, null);
    }

    /**
     * remove Closeable
     * 
     * @param closeable Closeable
     */
    public synchronized void remove(Closeable closeable) {
        closeableMap.remove(closeable);
    }

    /**
     * close all Closeable
     * 
     * @return Exception list if close error occurs
     */
    public synchronized List<Throwable> close() {
        List<Throwable> result = null;
        for (var i = closeableMap.keySet().iterator(); i.hasNext();) {
            var closeable = i.next();
            i.remove();

            try {
                closeable.close();
            } catch (Exception e) {
                if (result == null) {
                    result = new ArrayList<>();
                }
                result.add(e);
            }
        }
        return (result != null) ? result : List.of();
    }
}
