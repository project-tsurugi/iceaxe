package com.tsurugidb.iceaxe;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.session.event.logging.file.TsurugiSessionTxFileLogConfig;
import com.tsurugidb.iceaxe.session.event.logging.file.TsurugiSessionTxFileLogger;
import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi Connector.
 */
public class TsurugiConnector {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiConnector.class);

    /**
     * create connector.
     *
     * @param endpoint the end-point URI
     * @return connector
     */
    public static TsurugiConnector of(String endpoint) {
        var uri = URI.create(endpoint);
        return of(uri, NullCredential.INSTANCE, TgSessionOption.of());
    }

    /**
     * create connector.
     *
     * @param endpoint the end-point URI
     * @return connector
     */
    public static TsurugiConnector of(URI endpoint) {
        return of(endpoint, NullCredential.INSTANCE, TgSessionOption.of());
    }

    /**
     * create connector.
     *
     * @param endpoint   the end-point URI
     * @param credential credential. if null, use NullCredential
     * @return connector
     */
    public static TsurugiConnector of(String endpoint, Credential credential) {
        var uri = URI.create(endpoint);
        return of(uri, credential, TgSessionOption.of());
    }

    /**
     * create connector.
     *
     * @param endpoint   the end-point URI
     * @param credential credential. if null, use NullCredential
     * @return connector
     */
    public static TsurugiConnector of(URI endpoint, Credential credential) {
        return of(endpoint, credential, TgSessionOption.of());
    }

    /**
     * create connector.
     *
     * @param endpoint      the end-point URI
     * @param credential    credential. if null, use NullCredential
     * @param sessionOption session option. if null, use new SessionOption instance
     * @return connector
     */
    public static TsurugiConnector of(@Nonnull String endpoint, @Nullable Credential credential, @Nullable TgSessionOption sessionOption) {
        var uri = URI.create(endpoint);
        return of(uri, credential, sessionOption);
    }

    /**
     * create connector.
     *
     * @param endpoint      the end-point URI
     * @param credential    credential. if null, use NullCredential
     * @param sessionOption session option. if null, use new SessionOption instance
     * @return connector
     */
    public static TsurugiConnector of(@Nonnull URI endpoint, @Nullable Credential credential, @Nullable TgSessionOption sessionOption) {
        if (endpoint == null) {
            throw new IllegalArgumentException("endpoint is null");
        }
        var credential0 = (credential != null) ? credential : NullCredential.INSTANCE;
        var sessionOption0 = (sessionOption != null) ? sessionOption : TgSessionOption.of();

        var lowConnector = Connector.create(endpoint);
        var connector = new TsurugiConnector(lowConnector, endpoint, credential0, sessionOption0);
        return connector;
    }

    /** low connector */
    protected final Connector lowConnector;
    private final URI endpoint;
    private final Credential defaultCredential;
    private final TgSessionOption defaultSessionOption;
    private List<Consumer<TsurugiSession>> eventListenerList = null;
    private TsurugiSessionTxFileLogConfig txFileLogConfig = TsurugiSessionTxFileLogConfig.DEFAULT;

    /**
     * Creates a new instance.
     *
     * @param lowConnector         low connector
     * @param endpoint             end-point
     * @param defaultCredential    default credential
     * @param defaultSessionOption default session option
     */
    protected TsurugiConnector(Connector lowConnector, URI endpoint, Credential defaultCredential, TgSessionOption defaultSessionOption) {
        this.lowConnector = lowConnector;
        this.endpoint = endpoint;
        this.defaultCredential = defaultCredential;
        this.defaultSessionOption = defaultSessionOption;
    }

    /**
     * get end-point.
     *
     * @return end-point URI
     */
    public URI getEndpoint() {
        return this.endpoint;
    }

    /**
     * get session option.
     *
     * @return session option
     */
    public TgSessionOption getSessionOption() {
        return this.defaultSessionOption;
    }

    /**
     * add event listener.
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
     * set {@link TsurugiSessionTxFileLogger} config.
     *
     * @param config config
     */
    public void setTxFileLogConfig(@Nullable TsurugiSessionTxFileLogConfig config) {
        this.txFileLogConfig = config;
    }

    /**
     * create session.
     *
     * @return session
     * @throws IOException
     */
    public TsurugiSession createSession() throws IOException {
        return createSession(defaultCredential, defaultSessionOption);
    }

    /**
     * create session.
     *
     * @param credential credential
     * @return session
     * @throws IOException
     */
    public TsurugiSession createSession(Credential credential) throws IOException {
        return createSession(credential, defaultSessionOption);
    }

    /**
     * create session.
     *
     * @param sessionOption session option
     * @return session
     * @throws IOException
     */
    public TsurugiSession createSession(TgSessionOption sessionOption) throws IOException {
        return createSession(defaultCredential, sessionOption);
    }

    /**
     * create session.
     *
     * @param credential    credential
     * @param sessionOption session option
     * @return session
     * @throws IOException
     */
    public TsurugiSession createSession(Credential credential, TgSessionOption sessionOption) throws IOException {
        LOG.trace("create session. credential={}, option={}", credential, sessionOption);
        var option = (sessionOption != null) ? sessionOption : TgSessionOption.of();
        var lowSessionFuture = createLowSession(credential, option);
        var session = new TsurugiSession(lowSessionFuture, option);

        if (this.txFileLogConfig != null) {
            session.addEventListener(new TsurugiSessionTxFileLogger(txFileLogConfig));
        }

        event(listener -> listener.accept(session));
        return session;
    }

    /**
     * create low session.
     *
     * @param credential    credential
     * @param sessionOption session option
     * @return future of session
     * @throws IOException
     */
    protected FutureResponse<? extends Session> createLowSession(@Nullable Credential credential, TgSessionOption sessionOption) throws IOException {
        var lowBuilder = SessionBuilder.connect(lowConnector);

        if (credential != null) {
            lowBuilder.withCredential(credential);
        }

        return lowBuilder.createAsync();
    }

    @Override
    public String toString() {
        return "TsurugiConnector(" + endpoint + ", " + defaultCredential + ")";
    }
}
