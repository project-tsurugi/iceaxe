package com.tsurugidb.iceaxe.test.low;

import com.tsurugidb.tsubakuro.sql.PreparedStatement;

public class TestPreparedStatement extends TestServerResource implements PreparedStatement {

    private final boolean isQuery;

    public TestPreparedStatement(boolean query) {
        this.isQuery = query;
    }

    @Override
    public boolean hasResultRecords() {
        return this.isQuery;
    }
}
