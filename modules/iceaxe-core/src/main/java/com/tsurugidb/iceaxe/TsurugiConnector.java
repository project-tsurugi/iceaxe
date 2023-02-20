package com.tsurugidb.iceaxe;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.session.event.logging.file.TsurugiSessionTxFileLogConfig;
import com.tsurugidb.iceaxe.session.event.logging.file.TsurugiSessionTxFileLogConfig.TgTxFileLogDirectoryType;
import com.tsurugidb.iceaxe.session.event.logging.file.TsurugiSessionTxFileLogger;
import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;

/**
 * Tsurugi Connector
 */
public class TsurugiConnector {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiConnector.class);

    private static TsurugiSessionTxFileLogConfig txFileLogConfig;
    static {
        Path logDir = null;
        try {
            String s = System.getProperty("iceaxe.tx.log.dir"); //$NON-NLS-1$
            if (s != null) {
                logDir = Path.of(s);
            }
        } catch (Exception e) {
            LOG.warn("iceaxe.tx.log.dir error (ignore)", e);
        }
        if (logDir != null) {
            var config = TsurugiSessionTxFileLogConfig.of(logDir);
            try {
                String s = System.getProperty("iceaxe.tx.log.dir_type"); //$NON-NLS-1$
                if (s != null) {
                    config.directoryType(TgTxFileLogDirectoryType.valueOf(s.toUpperCase()));
                }
            } catch (Exception e) {
                LOG.warn("iceaxe.tx.log.dir_type error (ignore)", e);
            }
            try {
                String s = System.getProperty("iceaxe.tx.log.auto_flush"); //$NON-NLS-1$
                if (s != null) {
                    config.autoFlush(Boolean.parseBoolean(s));
                }
            } catch (Exception e) {
                LOG.warn("iceaxe.tx.log.auto_flush error (ignore)", e);
            }
            try {
                String s = System.getProperty("iceaxe.tx.log.explain"); //$NON-NLS-1$
                if (s != null) {
                    config.writeExplain(Integer.parseInt(s));
                }
            } catch (Exception e) {
                LOG.warn("iceaxe.tx.log.explain error (ignore)", e);
            }
            try {
                String s = System.getProperty("iceaxe.tx.log.record"); //$NON-NLS-1$
                if (s != null) {
                    config.writeReadRecord(Boolean.parseBoolean(s));
                }
            } catch (Exception e) {
                LOG.warn("iceaxe.tx.log.record error (ignore)", e);
            }
            txFileLogConfig = config;
            LOG.debug("iceaxe.tx.log={}", txFileLogConfig);
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
        if (txFileLogConfig != null) {
            session.addEventListener(new TsurugiSessionTxFileLogger(txFileLogConfig));
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
