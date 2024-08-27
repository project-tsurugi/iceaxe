/*
 * Copyright 2023-2024 Project Tsurugi.
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
package com.tsurugidb.iceaxe.sql.explain;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.StatementMetadata;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi statement metadata helper.
 */
public class TsurugiExplainHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiExplainHelper.class);

    /**
     * Retrieves execution plan of the statement.
     *
     * @param session tsurugi session
     * @param source  SQL
     * @return statement metadata
     * @throws IOException          if an I/O error occurs while retrieving statement metadata
     * @throws InterruptedException if interrupted while retrieving statement metadata
     */
    public TgStatementMetadata explain(TsurugiSession session, String source) throws IOException, InterruptedException {
        var sessionOption = session.getSessionOption();
        var connectTimeout = getConnectTimeout(sessionOption);
        var closeTimeout = getCloseTimeout(sessionOption);
        return explain(session, source, connectTimeout, closeTimeout);
    }

    /**
     * get connect timeout.
     *
     * @param sessionOption session option
     * @return timeout
     */
    protected IceaxeTimeout getConnectTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.EXPLAIN_CONNECT);
    }

    /**
     * get close timeout.
     *
     * @param sessionOption session option
     * @return timeout
     */
    @Deprecated(since = "1.4.0")
    protected IceaxeTimeout getCloseTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.EXPLAIN_CLOSE);
    }

    /**
     * Retrieves execution plan of the statement.
     *
     * @param session        tsurugi session
     * @param source         SQL
     * @param connectTimeout connect timeout
     * @param closeTimeout   close timeout
     * @return statement metadata
     * @throws IOException          if an I/O error occurs while retrieving statement metadata
     * @throws InterruptedException if interrupted while retrieving statement metadata
     */
    public TgStatementMetadata explain(TsurugiSession session, String source, IceaxeTimeout connectTimeout, IceaxeTimeout closeTimeout) throws IOException, InterruptedException {
        var lowSqlClient = session.getLowSqlClient();
        LOG.trace("explain start. source={}", source);
        var lowStatementMetadataFuture = explainLow(lowSqlClient, source);
        LOG.trace("explain started");
        return getStatementMetadata(session, source, null, lowStatementMetadataFuture, connectTimeout);
    }

    /**
     * explain.
     *
     * @param lowSqlClient low SQL client
     * @param source       SQL
     * @return future of statement metadata
     * @throws IOException if an I/O error occurs while retrieving statement metadata
     */
    protected FutureResponse<StatementMetadata> explainLow(SqlClient lowSqlClient, String source) throws IOException {
        return lowSqlClient.explain(source);
    }

    /**
     * Retrieves execution plan of the statement.
     *
     * @param session        tsurugi session
     * @param source         SQL
     * @param arguments      SQL arguments
     * @param lowPs          PreparedStatement
     * @param lowParameter   list of Parameter
     * @param connectTimeout connect timeout
     * @param closeTimeout   close timeout
     * @return statement metadata
     * @throws IOException          if an I/O error occurs while retrieving statement metadata
     * @throws InterruptedException if interrupted while retrieving statement metadata
     */
    public TgStatementMetadata explain(TsurugiSession session, String source, Object arguments, PreparedStatement lowPs, List<Parameter> lowParameter, IceaxeTimeout connectTimeout,
            IceaxeTimeout closeTimeout) throws IOException, InterruptedException {
        var lowSqlClient = session.getLowSqlClient();
        LOG.trace("explain start. source={}", source);
        var lowStatementMetadataFuture = explainLow(lowSqlClient, lowPs, lowParameter);
        LOG.trace("explain started");
        return getStatementMetadata(session, source, arguments, lowStatementMetadataFuture, connectTimeout);
    }

    /**
     * explain.
     *
     * @param lowSqlClient low SQL client
     * @param lowPs        prepared statement
     * @param lowParameter list of parameter
     * @return future of statement metadata
     * @throws IOException if an I/O error occurs while retrieving statement metadata
     */
    protected FutureResponse<StatementMetadata> explainLow(SqlClient lowSqlClient, PreparedStatement lowPs, List<Parameter> lowParameter) throws IOException {
        return lowSqlClient.explain(lowPs, lowParameter);
    }

    /**
     * get statement metadata.
     *
     * @param session                    session
     * @param source                     SQL
     * @param arguments                  SQL parameter
     * @param lowStatementMetadataFuture future of statement metadata
     * @param connectTimeout             connect timeout
     * @return statement metadata
     * @throws IOException          if an I/O error occurs while retrieving statement metadata
     * @throws InterruptedException if interrupted while retrieving statement metadata
     */
    protected TgStatementMetadata getStatementMetadata(TsurugiSession session, String source, Object arguments, FutureResponse<StatementMetadata> lowStatementMetadataFuture,
            IceaxeTimeout connectTimeout) throws IOException, InterruptedException {
        var lowStatementMetadata = IceaxeIoUtil.getAndCloseFuture(lowStatementMetadataFuture, //
                connectTimeout, IceaxeErrorCode.EXPLAIN_CONNECT_TIMEOUT, //
                IceaxeErrorCode.EXPLAIN_CLOSE_TIMEOUT);
        LOG.trace("explain end");

        return newStatementMetadata(source, arguments, lowStatementMetadata);
    }

    /**
     * Creates a new statement metadata.
     *
     * @param source               SQL
     * @param arguments            SQL parameter
     * @param lowStatementMetadata low statement metadata
     * @return statement metadata
     */
    protected TgStatementMetadata newStatementMetadata(String source, Object arguments, StatementMetadata lowStatementMetadata) {
        return new TgStatementMetadata(source, arguments, lowStatementMetadata);
    }
}
