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
package com.tsurugidb.iceaxe.sql.type;

import java.io.IOException;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.iceaxe.util.IceaxeTimeoutCloseable;
import com.tsurugidb.tsubakuro.common.LargeObjectInfo;

/**
 * Tsurugi uploaded CLOB.
 *
 * <p>
 * TgRemoteClob is closed after the SQL statement is executed ({@link TsurugiTransaction#executeStatement(com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement, Object) executeStatement()},
 * {@link TsurugiTransaction#executeQuery(com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery, Object) executeQuery()}) and after the execution plan is retrieved
 * ({@link TsurugiSqlPreparedStatement#explain(Object)}, {@link TsurugiSqlPreparedQuery#explain(Object)}). If neither of these operations is performed, it is closed when {@link TsurugiSession} is
 * closed.
 * </p>
 *
 * @since 1.16.0
 */
public interface TgRemoteClob extends IceaxeTimeoutCloseable {

    /**
     * get low-level LargeObjectInfo.
     *
     * @return low-level LargeObjectInfo
     */
    @IceaxeInternal
    public LargeObjectInfo getLowLargeObjectInfo();

    @Override
    void close() throws IOException;

    @Override
    void close(long timeoutNanos) throws IOException;
}
