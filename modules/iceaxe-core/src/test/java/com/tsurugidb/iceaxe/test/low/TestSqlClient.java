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
package com.tsurugidb.iceaxe.test.low;

import java.io.IOException;
import java.util.Collection;

import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;
import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.StatementMetadata;
import com.tsurugidb.tsubakuro.sql.TableList;
import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.util.FutureResponse;

public class TestSqlClient extends TestServerResource implements SqlClient {

    private final Session ownerSession;

    private FutureResponse<TableList> tableListFuture;
    private FutureResponse<TableMetadata> tableMetadataFuture;
    private FutureResponse<StatementMetadata> statementMetadataFuture;
    private FutureResponse<PreparedStatement> preparedStatementFuture;
    private FutureResponse<Transaction> transactionFuture;

    public TestSqlClient(Session owner) {
        this.ownerSession = owner;
    }

    public Session getSession() {
        return this.ownerSession;
    }

    public void setTestTableListFutureResponse(FutureResponse<TableList> future) {
        this.tableListFuture = future;
    }

    @Override
    public FutureResponse<TableList> listTables() throws IOException {
        return this.tableListFuture;
    }

    public void setTestTableMetadataFutureResponse(FutureResponse<TableMetadata> future) {
        this.tableMetadataFuture = future;
    }

    @Override
    public FutureResponse<TableMetadata> getTableMetadata(String tableName) throws IOException {
        return this.tableMetadataFuture;
    }

    public void setTestStatementMetadataFutureResponse(FutureResponse<StatementMetadata> future) {
        this.statementMetadataFuture = future;
    }

    @Override
    public FutureResponse<StatementMetadata> explain(String source) throws IOException {
        return this.statementMetadataFuture;
    }

    @Override
    public FutureResponse<StatementMetadata> explain(PreparedStatement statement, Collection<? extends Parameter> parameters) throws IOException {
        return this.statementMetadataFuture;
    }

    @Override
    public FutureResponse<StatementMetadata> explain(PreparedStatement statement, Parameter... parameters) throws IOException {
        return this.statementMetadataFuture;
    }

    public void setTestPreparedStatementFutureResponse(FutureResponse<PreparedStatement> future) {
        this.preparedStatementFuture = future;
    }

    @Override
    public FutureResponse<PreparedStatement> prepare(String source, Collection<? extends Placeholder> placeholders) throws IOException {
        return this.preparedStatementFuture;
    }

    public void setTestTransactionFutureResponse(FutureResponse<Transaction> future) {
        this.transactionFuture = future;
    }

    @Override
    public FutureResponse<Transaction> createTransaction(TransactionOption option) throws IOException {
        if (this.transactionFuture == null) {
            return new TestFutureResponse<>() {
                @Override
                protected Transaction getInternal() {
                    return new TestLowTransaction();
                }
            };
        }
        return this.transactionFuture;
    }
}
