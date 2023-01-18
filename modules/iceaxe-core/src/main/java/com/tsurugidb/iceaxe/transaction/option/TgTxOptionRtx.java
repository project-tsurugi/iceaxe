package com.tsurugidb.iceaxe.transaction.option;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.sql.proto.SqlRequest.TransactionType;

/**
 * Tsurugi Transaction Option (RTX)
 */
@ThreadSafe
public class TgTxOptionRtx extends AbstractTgTxOptionLong<TgTxOptionRtx> {

    @Override
    public String typeName() {
        return "RTX";
    }

    @Override
    public TransactionType type() {
        return TransactionType.READ_ONLY;
    }
}
