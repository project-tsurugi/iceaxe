package com.tsurugidb.iceaxe;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.session.event.logging.file.TsurugiSessionTxFileLogConfig;
import com.tsurugidb.iceaxe.session.event.logging.file.TsurugiSessionTxFileLogConfig.TgTxFileLogSubDirType;
import com.tsurugidb.iceaxe.session.event.logging.file.TsurugiSessionTxFileLogger;
import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.util.FutureResponse;

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
            txLogConfig("iceaxe.tx.log.sub_dir", s -> config.subDirType(TgTxFileLogSubDirType.valueOf(s.toUpperCase()))); // $NON-NLS-1$
            txLogConfig("iceaxe.tx.log.write_sql_file", s -> config.writeSqlFile(Boolean.parseBoolean(s))); // $NON-NLS-1$
            txLogConfig("iceaxe.tx.log.header_format", s -> config.headerFormatter(DateTimeFormatter.ofPattern(s))); // $NON-NLS-1$
            txLogConfig("iceaxe.tx.log.sql_max_length", s -> config.sqlMaxLength(Integer.parseInt(s))); // $NON-NLS-1$
            txLogConfig("iceaxe.tx.log.arg_max_length", s -> config.argMaxLength(Integer.parseInt(s))); // $NON-NLS-1$
            txLogConfig("iceaxe.tx.log.explain", s -> config.writeExplain(Integer.parseInt(s))); // $NON-NLS-1$
            txLogConfig("iceaxe.tx.log.record", s -> config.writeReadRecord(Boolean.parseBoolean(s))); // $NON-NLS-1$
            txLogConfig("iceaxe.tx.log.read_progress", s -> config.readProgress(Integer.parseInt(s))); // $NON-NLS-1$
            txLogConfig("iceaxe.tx.log.auto_flush", s -> config.autoFlush(Boolean.parseBoolean(s))); // $NON-NLS-1$
            txFileLogConfig = config;
            LOG.debug("iceaxe.tx.log={}", txFileLogConfig);
        }
    }

    private static void txLogConfig(String key, Consumer<String> configSetter) {
        try {
            String s = System.getProperty(key);
            if (s != null) {
                configSetter.accept(s.trim());
            }
        } catch (Exception e) {
            LOG.warn(key + " error (ignore)", e);
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
    protected final Connector lowConnector;
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
        var lowSessionFuture = createLowSession(info);
        var session = new TsurugiSession(info, lowSessionFuture);
        if (txFileLogConfig != null) {
            session.addEventListener(new TsurugiSessionTxFileLogger(txFileLogConfig));
        }
        event(listener -> listener.accept(session));
        return session;
    }

    protected FutureResponse<? extends Session> createLowSession(TgSessionInfo info) throws IOException {
        var lowBuilder = SessionBuilder.connect(lowConnector);

        var credential = info.credential();
        if (credential != null) {
            lowBuilder.withCredential(credential);
        }

        return lowBuilder.createAsync();
    }

    @Override
    public String toString() {
        return "TsurugiConnector(" + endpoint + ")";
    }
}
