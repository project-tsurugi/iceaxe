package com.tsurugidb.iceaxe.example;

import java.util.List;
import java.util.stream.Stream;

import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.transaction.option.TgTxOptionLtx;
import com.tsurugidb.iceaxe.transaction.option.TgTxOptionOcc;
import com.tsurugidb.iceaxe.transaction.option.TgTxOptionRtx;

/**
 * TgTxOption example.
 */
public class Example03TxOption {

    // OCC (SHORT)
    TgTxOptionOcc occ() {
        return TgTxOption.ofOCC();
    }

    // LONG
    // LTXの場合は、writePreserveに更新対象テーブルを全て指定する
    TgTxOptionLtx ltx1() {
        return TgTxOption.ofLTX("tableName1", "tableName2");
    }

    TgTxOptionLtx ltx2() {
        var writePreserve = List.of("tableName1", "tableName2");
        return TgTxOption.ofLTX(writePreserve);
    }

    TgTxOptionLtx ltx3() {
        var writePreserve = Stream.of("tableName1", "tableName2");
        return TgTxOption.ofLTX(writePreserve);
    }

    TgTxOptionLtx ltx4() {
        return TgTxOption.ofLTX().addWritePreserve("tableName1").addWritePreserve("tableName2");
    }

    // READ ONLY
    TgTxOptionRtx rtx() {
        return TgTxOption.ofRTX();
        // TODO including, excluding, etc
    }

    // label
    TgTxOption label() {
        return TgTxOption.ofOCC().label("transaction label");
    }

    // clone
    TgTxOption cloneSimple(TgTxOption option) {
        return option.clone();
    }

    TgTxOption cloneWithLabel(TgTxOption option) {
        return option.clone("new label");
    }
}
