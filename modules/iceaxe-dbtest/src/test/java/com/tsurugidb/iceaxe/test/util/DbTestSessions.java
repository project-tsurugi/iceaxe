package com.tsurugidb.iceaxe.test.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TsurugiSession;

public class DbTestSessions implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(DbTestSessions.class);

    private final long timeout;
    private final TimeUnit timeUnit;
    private final List<TsurugiSession> sessionList = new ArrayList<>();

    public DbTestSessions() {
        this.timeout = 0;
        this.timeUnit = null;
    }

    public DbTestSessions(long timeout, TimeUnit unit) {
        this.timeout = timeout;
        this.timeUnit = unit;
    }

    public TsurugiSession createSession() throws IOException {
        var session = (timeUnit == null) ? DbTestConnector.createSession() //
                : DbTestConnector.createSession(timeout, timeUnit);
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
