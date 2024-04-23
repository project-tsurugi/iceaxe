package com.tsurugidb.iceaxe.test;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.iceaxe.test.low.TestLowSession;
import com.tsurugidb.iceaxe.test.low.TestSqlClient;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.sql.SqlClient;

public class TestTsurugiSession extends TsurugiSession {

    public TestTsurugiSession(TgSessionOption sessionOption) {
        super(new TestFutureResponse<>() {
            @Override
            protected Session getInternal() {
                return new TestLowSession();
            }
        }, sessionOption);
    }

    @Override
    protected SqlClient newSqlClient(Session lowSession) {
        return new TestSqlClient(lowSession);
    }
}
