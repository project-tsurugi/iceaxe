package com.tsurugidb.iceaxe.example;

import java.util.List;

import com.tsurugidb.iceaxe.transaction.TgTxOption;

/**
 * TgTxOption example.
 */
public class Example03TransactionOption {

    // OCC (SHORT)
    TgTxOption occ() {
        return TgTxOption.ofOCC();
    }

    // LONG
    // LTXの場合は、writePreserveに更新対象テーブルを全て指定する
    TgTxOption longTransaction1() {
        return TgTxOption.ofLTX("tableName1", "tableName2");
    }

    TgTxOption longTransaction2() {
        var writePreserve = List.of("tableName1", "tableName2");
        return TgTxOption.ofLTX(writePreserve);
    }

    TgTxOption longTransaction3() {
        return TgTxOption.ofLTX().addWritePreserve("tableName1").addWritePreserve("tableName2");
    }

    // READ ONLY
    TgTxOption readOnlyTransaction() {
        return TgTxOption.ofRTX();
        // TODO including, excluding, etc
    }
}
