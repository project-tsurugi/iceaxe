package com.tsurugidb.iceaxe.transaction.option;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.sql.proto.SqlRequest.TransactionType;

/**
 * Tsurugi Transaction Option (OCC)
 */
@ThreadSafe
public class TgTxOptionOcc extends AbstractTgTxOption<TgTxOptionOcc> {

    @Override
    public String typeName() {
        return "OCC";
    }

    @Override
    public TransactionType type() {
        return TransactionType.SHORT;
    }
}
