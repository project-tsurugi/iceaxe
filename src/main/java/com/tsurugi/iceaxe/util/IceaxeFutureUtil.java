package com.tsurugi.iceaxe.util;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import com.tsurugi.iceaxe.session.TgSessionInfo;

// internal
public class IceaxeFutureUtil {

    public static <T> T getFromFuture(Future<T> future, TgSessionInfo info) throws IOException {
        try {
            return future.get(info.timeoutTime(), info.timeoutUnit());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            Throwable c = e.getCause();
            if (c instanceof IOException) {
                throw (IOException) c;
            }
            throw new IOException(e);
        }
    }
}
