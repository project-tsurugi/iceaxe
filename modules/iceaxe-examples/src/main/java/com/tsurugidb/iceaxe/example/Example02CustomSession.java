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
package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.OverridingMethodsMustInvokeSuper;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgEntityParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.util.InterruptedRuntimeException;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * custom TsurugiSession example
 */
public class Example02CustomSession {

    public static class PsPoolTsurugiSession extends TsurugiSession {

        private final Map<String, TsurugiSqlQuery<?>> queryMap = new ConcurrentHashMap<>();
        private final Map<String, TsurugiSqlPreparedQuery<?, ?>> preparedQueryMap = new ConcurrentHashMap<>();
        private final Map<String, TsurugiSqlStatement> statementMap = new ConcurrentHashMap<>();
        private final Map<String, TsurugiSqlPreparedStatement<?>> preparedStatementMap = new ConcurrentHashMap<>();

        public PsPoolTsurugiSession(FutureResponse<? extends Session> lowSessionFuture, TgSessionOption sessionOption) {
            super(lowSessionFuture, sessionOption);
        }

        public TsurugiSqlQuery<TsurugiResultEntity> getOrCreateQuery(String sql) throws IOException {
            return getOrCreateQuery(sql, TgResultMapping.DEFAULT);
        }

        public <R> TsurugiSqlQuery<R> getOrCreateQuery(String sql, TgResultMapping<R> resultMapping) throws IOException {
            try {
                @SuppressWarnings("unchecked")
                var query = (TsurugiSqlQuery<R>) queryMap.computeIfAbsent(sql, key -> {
                    try {
                        return createQuery(key, resultMapping);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                return query;
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }

        public <P> TsurugiSqlPreparedQuery<P, TsurugiResultEntity> getOrCreateQuery(String sql, TgParameterMapping<P> parameterMapping) throws IOException, InterruptedException {
            return getOrCreateQuery(sql, parameterMapping, TgResultMapping.DEFAULT);
        }

        public <P, R> TsurugiSqlPreparedQuery<P, R> getOrCreateQuery(String sql, TgParameterMapping<P> parameterMapping, TgResultMapping<R> resultMapping) throws IOException, InterruptedException {
            try {
                @SuppressWarnings("unchecked")
                var query = (TsurugiSqlPreparedQuery<P, R>) preparedQueryMap.computeIfAbsent(sql, key -> {
                    try {
                        return createQuery(key, parameterMapping, resultMapping);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } catch (InterruptedException e) {
                        throw new InterruptedRuntimeException(e);
                    }
                });
                return query;
            } catch (UncheckedIOException e) {
                throw e.getCause();
            } catch (InterruptedRuntimeException e) {
                throw e.getCause();
            }
        }

        public TsurugiSqlStatement getOrCreateStatement(String sql) throws IOException {
            try {
                var statement = statementMap.computeIfAbsent(sql, key -> {
                    try {
                        return createStatement(key);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                return statement;
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }

        public <P> TsurugiSqlPreparedStatement<P> getOrCreateStatement(String sql, TgParameterMapping<P> parameterMapping) throws IOException, InterruptedException {
            try {
                @SuppressWarnings("unchecked")
                var statement = (TsurugiSqlPreparedStatement<P>) preparedStatementMap.computeIfAbsent(sql, key -> {
                    try {
                        return createStatement(key, parameterMapping);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } catch (InterruptedException e) {
                        throw new InterruptedRuntimeException(e);
                    }
                });
                return statement;
            } catch (UncheckedIOException e) {
                throw e.getCause();
            } catch (InterruptedRuntimeException e) {
                throw e.getCause();
            }
        }

        @Override
        @OverridingMethodsMustInvokeSuper
        public void close() throws IOException, InterruptedException {
            queryMap.clear();
            preparedQueryMap.clear();
            statementMap.clear();
            preparedStatementMap.clear();
            super.close();
        }
    }

    public static void main(String... args) throws Exception {
        String endpoint = "tcp://localhost:12345";
        var connector = TsurugiConnector.of(endpoint);
        connector.setSesionGenerator(PsPoolTsurugiSession::new);

        try (var session = connector.createSession()) {
            var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);

            tm.execute(transaction -> {
                executeInTransaction(transaction);
            });
        }
    }

    private static final String TEST_UPSERT_SQL = "insert or replace into test values(:foo, :bar, :zzz)";
    private static final TgEntityParameterMapping<TestEntity> TEST_UPSERT_MAPPING = TgParameterMapping.of(TestEntity.class) //
            .addInt("foo", TestEntity::getFoo) //
            .addLong("bar", TestEntity::getBar) //
            .addString("zzz", TestEntity::getZzz);

    static void executeInTransaction(TsurugiTransaction transaction) throws IOException, InterruptedException, TsurugiTransactionException {
        var session = (PsPoolTsurugiSession) transaction.getSession();
        var ps = session.getOrCreateStatement(TEST_UPSERT_SQL, TEST_UPSERT_MAPPING);
        for (int i = 0; i < 10; i++) {
            var entity = new TestEntity(i, (long) i, Integer.toString(i));
            transaction.executeAndGetCountDetail(ps, entity);
        }
    }
}
