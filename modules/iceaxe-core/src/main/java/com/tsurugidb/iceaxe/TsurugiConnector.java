package com.tsurugidb.iceaxe;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.session.event.logging.file.TsurugiSessionTxFileLogger;
import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;

/**
 * Tsurugi Connector
 */
public class TsurugiConnector {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiConnector.class);

    private static Path sessionTxLogDir;
    private static int sessionTxLogExplain;
    static {
        try {
            String dir = System.getProperty("iceaxe.tx.log.dir"); //$NON-NLS-1$
            if (dir != null) {
                sessionTxLogDir = Path.of(dir);
                Files.createDirectories(sessionTxLogDir);
                LOG.debug("sessionTxLogDir={}", sessionTxLogDir);
            }
        } catch (Exception ignore) {
            sessionTxLogDir = null;
        }
        try {
            String explain = System.getProperty("iceaxe.tx.log.explain"); //$NON-NLS-1$
            if (explain != null) {
                sessionTxLogExplain = Integer.parseInt(explain);
                LOG.debug("sessionTxLogExplain={}", sessionTxLogExplain);
            } else {
                sessionTxLogExplain = TsurugiSessionTxFileLogger.EXPLAIN_FILE;
            }
        } catch (Exception ignore) {
            sessionTxLogExplain = TsurugiSessionTxFileLogger.EXPLAIN_FILE;
        }
    }

    /**
     * create Tsurugi Connector
     *
     * @param endpoint the end-point URI
     * @return Tsurugi Connector
     */
    public static TsurugiConnector createConnector(String endpoint) {
        var uri = URI.create(endpoint);
        return createConnector(uri);
    }

    /**
     * create Tsurugi Connector
     *
     * @param endpoint the end-point URI
     * @return Tsurugi Connector
     */
    public static TsurugiConnector createConnector(URI endpoint) {
        var lowConnector = Connector.create(endpoint);
        var connector = new TsurugiConnector(endpoint, lowConnector);
        return connector;
    }

    private final URI endpoint;
    private final Connector lowConnector;
    private List<Consumer<TsurugiSession>> eventListenerList = null;

    protected TsurugiConnector(URI endpoint, Connector lowConnector) {
        this.endpoint = endpoint;
        this.lowConnector = lowConnector;
    }

    /**
     * get end-point
     *
     * @return end-point URI
     */
    public URI getEndpoint() {
        return this.endpoint;
    }

    /**
     * add event listener
     *
     * @param listener event listener
     * @return this
     */
    public TsurugiConnector addEventListener(Consumer<TsurugiSession> listener) {
        if (this.eventListenerList == null) {
            this.eventListenerList = new ArrayList<>();
        }
        eventListenerList.add(listener);
        return this;
    }

    private void event(Consumer<Consumer<TsurugiSession>> action) {
        if (this.eventListenerList != null) {
            for (var listener : eventListenerList) {
                action.accept(listener);
            }
        }
    }

    /**
     * create Tsurugi Session
     *
     * @param info Session Information
     * @return Tsurugi Session
     * @throws IOException
     */
    public TsurugiSession createSession(TgSessionInfo info) throws IOException {
        LOG.trace("session create. info={}", info);
        var lowSession = createLowSession();
        var lowCredential = info.credential();
        var lowWireFuture = lowConnector.connect(lowCredential);
        var session = new TsurugiSession(info, lowSession, lowWireFuture);
        if (sessionTxLogDir != null) {
            session.addEventListener(new TsurugiSessionTxFileLogger(sessionTxLogDir, sessionTxLogExplain));
        }
        event(listener -> listener.accept(session));
        return session;
    }

    protected Session createLowSession() {
        return new SessionImpl();
    }

    @Override
    public String toString() {
        return "TsurugiConnector(" + endpoint + ")";
    }
}
