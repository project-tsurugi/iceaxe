package com.tsurugidb.iceaxe.test.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TsurugiSession;

public class DbTestSessions implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(DbTestSessions.class);

    private final List<TsurugiSession> sessionList = new ArrayList<>();

    public TsurugiSession createSession() throws IOException {
        var session = DbTestConnector.createSession();
        sessionList.add(session);
        return session;
    }

    @Override
    public void close() throws IOException {
        for (var session : sessionList) {
            try {
                session.close();
            } catch (Exception e) {
                LOG.warn("session close error", e.getMessage());
            }
        }
    }
}
