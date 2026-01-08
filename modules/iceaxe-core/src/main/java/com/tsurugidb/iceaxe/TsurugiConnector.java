/*
 * Copyright 2023-2026 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

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
     * @param applicationName application name
     * @param endpoint        the end-point URI
     * @return connector
     * @since 1.6.0
     */
    public static TsurugiConnector of(String applicationName, String endpoint) {
        var uri = URI.create(endpoint);
        return of(uri, NullCredential.INSTANCE, TgSessionOption.of()).setApplicationName(applicationName);
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
     * @param applicationName application name
     * @param endpoint        the end-point URI
     * @return connector
     * @since 1.6.0
     */
    public static TsurugiConnector of(String applicationName, URI endpoint) {
        return of(endpoint, NullCredential.INSTANCE, TgSessionOption.of()).setApplicationName(applicationName);
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
     * @param applicationName application name
     * @param endpoint        the end-point URI
     * @param credential      credential. if null, use NullCredential
     * @return connector
     * @since 1.6.0
     */
    public static TsurugiConnector of(String applicationName, String endpoint, Credential credential) {
        var uri = URI.create(endpoint);
        return of(uri, credential, TgSessionOption.of()).setApplicationName(applicationName);
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
     * @param applicationName application name
     * @param endpoint        the end-point URI
     * @param credential      credential. if null, use NullCredential
     * @return connector
     * @since 1.6.0
     */
    public static TsurugiConnector of(String applicationName, URI endpoint, Credential credential) {
        return of(endpoint, credential, TgSessionOption.of()).setApplicationName(applicationName);
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
     * @param applicationName application name
     * @param endpoint        the end-point URI
     * @param credential      credential. if null, use NullCredential
     * @param sessionOption   session option. if null, use new SessionOption instance
     * @return connector
     * @since 1.6.0
     */
    public static TsurugiConnector of(String applicationName, @Nonnull String endpoint, @Nullable Credential credential, @Nullable TgSessionOption sessionOption) {
        var uri = URI.create(endpoint);
        return of(uri, credential, sessionOption).setApplicationName(applicationName);
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

    /**
     * create connector.
     *
     * @param applicationName application name
     * @param endpoint        the end-point URI
     * @param credential      credential. if null, use NullCredential
     * @param sessionOption   session option. if null, use new SessionOption instance
     * @return connector
     * @since 1.6.0
     */
    public static TsurugiConnector of(String applicationName, @Nonnull URI endpoint, @Nullable Credential credential, @Nullable TgSessionOption sessionOption) {
        return of(endpoint, credential, sessionOption).setApplicationName(applicationName);
    }

    /** low connector */
    protected final Connector lowConnector;
    private final URI endpoint;
    private final Credential defaultCredential;
    private final TgSessionOption defaultSessionOption;
    private String defaultApplicationName = null;
    private BiFunction<FutureResponse<? extends Session>, TgSessionOption, ? extends TsurugiSession> sessionGenerator = null;
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

    Credential getCredential() {
        return this.defaultCredential;
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
     * set application name.
     *
     * @param name application name
     * @return this
     * @since 1.4.0
     */
    public TsurugiConnector setApplicationName(@Nullable String name) {
        this.defaultApplicationName = name;
        return this;
    }

    /**
     * get application name.
     *
     * @return application name
     * @since 1.4.0
     */
    public @Nullable String getApplicationName() {
        return this.defaultApplicationName;
    }

    /**
     * get application name.
     *
     * @param sessionOption session option
     * @return application name
     * @since 1.4.0
     */
    public Optional<String> findApplicationName(TgSessionOption sessionOption) {
        String name = null;
        if (sessionOption != null) {
            name = sessionOption.getApplicationName();
        }
        if (name == null) {
            name = this.defaultApplicationName;
        }
        return Optional.ofNullable(name);
    }

    /**
     * set session generator.
     *
     * @param generator session generator
     * @since 1.3.0
     */
    public void setSesionGenerator(BiFunction<FutureResponse<? extends Session>, TgSessionOption, ? extends TsurugiSession> generator) {
        this.sessionGenerator = generator;
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

    /**
     * find event listener.
     *
     * @param predicate predicate for event listener
     * @return event listener
     * @since 1.3.0
     */
    public Optional<Consumer<TsurugiSession>> findEventListener(Predicate<Consumer<TsurugiSession>> predicate) {
        var listenerList = this.eventListenerList;
        if (listenerList != null) {
            for (var listener : listenerList) {
                if (predicate.test(listener)) {
                    return Optional.of(listener);
                }
            }
        }
        return Optional.empty();
    }

    private void event(Consumer<Consumer<TsurugiSession>> action) {
        var listenerList = this.eventListenerList;
        if (listenerList != null) {
            for (var listener : listenerList) {
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
     * @throws IOException if an I/O error occurs during connection
     */
    public TsurugiSession createSession() throws IOException {
        String label = defaultSessionOption.getLabel();
        return createSession(label, defaultCredential, defaultSessionOption);
    }

    /**
     * create session.
     *
     * @param label session label
     * @return session
     * @throws IOException if an I/O error occurs during connection
     * @since 1.6.0
     */
    public TsurugiSession createSession(String label) throws IOException {
        return createSession(label, defaultCredential, defaultSessionOption);
    }

    /**
     * create session.
     *
     * @param credential credential
     * @return session
     * @throws IOException if an I/O error occurs during connection
     */
    public TsurugiSession createSession(Credential credential) throws IOException {
        String label = defaultSessionOption.getLabel();
        return createSession(label, credential, defaultSessionOption);
    }

    /**
     * create session.
     *
     * @param label      session label
     * @param credential credential
     * @return session
     * @throws IOException if an I/O error occurs during connection
     * @since 1.6.0
     */
    public TsurugiSession createSession(String label, Credential credential) throws IOException {
        return createSession(label, credential, defaultSessionOption);
    }

    /**
     * create session.
     *
     * @param sessionOption session option
     * @return session
     * @throws IOException if an I/O error occurs during connection
     */
    public TsurugiSession createSession(TgSessionOption sessionOption) throws IOException {
        String label = sessionOption.getLabel();
        return createSession(label, defaultCredential, sessionOption);
    }

    /**
     * create session.
     *
     * @param label         session label
     * @param sessionOption session option
     * @return session
     * @throws IOException if an I/O error occurs during connection
     * @since 1.6.0
     */
    public TsurugiSession createSession(String label, TgSessionOption sessionOption) throws IOException {
        return createSession(label, defaultCredential, sessionOption);
    }

    /**
     * create session.
     *
     * @param credential    credential
     * @param sessionOption session option
     * @return session
     * @throws IOException if an I/O error occurs during connection
     */
    public TsurugiSession createSession(Credential credential, TgSessionOption sessionOption) throws IOException {
        String label = sessionOption.getLabel();
        return createSession(label, credential, sessionOption);
    }

    /**
     * create session.
     *
     * @param label         session label
     * @param credential    credential
     * @param sessionOption session option
     * @return session
     * @throws IOException if an I/O error occurs during connection
     * @since 1.6.0
     */
    public TsurugiSession createSession(String label, Credential credential, TgSessionOption sessionOption) throws IOException {
        LOG.trace("create session. label={}, credential={}, option={}", label, credential, sessionOption);
        var option = (sessionOption != null) ? sessionOption : TgSessionOption.of();
        var lowSessionFuture = createLowSession(label, credential, option);
        var session = newTsurugiSession(lowSessionFuture, option);

        if (this.txFileLogConfig != null) {
            session.addEventListener(new TsurugiSessionTxFileLogger(txFileLogConfig));
        }

        event(listener -> listener.accept(session));
        return session;
    }

    /**
     * create low session.
     *
     * @param label         session label
     * @param credential    credential
     * @param sessionOption session option
     * @return future of session
     * @throws IOException if an I/O error occurs during connection
     */
    protected FutureResponse<? extends Session> createLowSession(@Nullable String label, @Nullable Credential credential, TgSessionOption sessionOption) throws IOException {
        var lowBuilder = createLowSessionBuilder(label, credential, sessionOption);
        return lowBuilder.createAsync();
    }

    /**
     * create low session builder.
     *
     * @param label         session label
     * @param credential    credential
     * @param sessionOption session option
     * @return session builder
     * @since 1.4.0
     */
    protected SessionBuilder createLowSessionBuilder(@Nullable String label, @Nullable Credential credential, TgSessionOption sessionOption) {
        var lowBuilder = SessionBuilder.connect(lowConnector);

        if (credential != null) {
            lowBuilder.withCredential(credential);
        }

        if (label != null) {
            lowBuilder.withLabel(label);
        }

        findApplicationName(sessionOption).ifPresent(name -> {
            lowBuilder.withApplicationName(name);
        });

        sessionOption.findKeepAlive().ifPresent(keepAlive -> {
            lowBuilder.withKeepAlive(keepAlive);
        });

        sessionOption.findLargeObjectPathMapping().ifPresent(mapping -> {
            lowBuilder.withBlobPathMapping(mapping);
        });

        return lowBuilder;
    }

    /**
     * create session instance.
     *
     * @param lowSessionFuture future of low session
     * @param sessionOption    session option
     * @return session
     * @since 1.3.0
     */
    protected TsurugiSession newTsurugiSession(FutureResponse<? extends Session> lowSessionFuture, TgSessionOption sessionOption) {
        var generator = this.sessionGenerator;
        if (generator != null) {
            return generator.apply(lowSessionFuture, sessionOption);
        }

        return new TsurugiSession(lowSessionFuture, sessionOption);
    }

    @Override
    public String toString() {
        return "TsurugiConnector(" + endpoint + ", " + defaultCredential + ")";
    }
}
