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
package com.tsurugidb.iceaxe.sql;

import java.io.IOException;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.util.IceaxeInternal;

/**
 * Tsurugi SQL definition (not prepared).
 */
public abstract class TsurugiSqlDirect extends TsurugiSql {

    /**
     * Creates a new instance.
     * <p>
     * Call {@link #initialize()} after construct.
     * </p>
     *
     * @param session session
     * @param sql     SQL
     */
    @IceaxeInternal
    protected TsurugiSqlDirect(TsurugiSession session, String sql) {
        super(session, sql);
    }

    /**
     * initialize.
     * <p>
     * Call this method only once after construct.
     * </p>
     *
     * @throws IOException if session already closed
     * @since 1.3.0
     */
    @IceaxeInternal
    @Override
    public void initialize() throws IOException {
        super.initialize();
    }

    @Override
    public final boolean isPrepared() {
        return false;
    }

    /**
     * Retrieves execution plan of the statement.
     *
     * @return statement metadata
     * @throws IOException          if an I/O error occurs while retrieving statement metadata
     * @throws InterruptedException if interrupted while retrieving statement metadata
     */
    public TgStatementMetadata explain() throws IOException, InterruptedException {
        var session = getSession();
        var helper = session.getExplainHelper();
        var connectTimeout = getExplainConnectTimeout();
        @SuppressWarnings("deprecation")
        var closeTimeout = getExplainCloseTimeout();
        return helper.explain(session, sql, connectTimeout, closeTimeout);
    }

    // close

    @Override
    public void close(long timeoutNanos) throws IOException, InterruptedException {
        close();
    }
}
