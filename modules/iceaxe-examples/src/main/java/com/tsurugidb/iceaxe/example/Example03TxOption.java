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
package com.tsurugidb.iceaxe.example;

import java.util.List;
import java.util.stream.Stream;

import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.transaction.option.TgTxOptionLtx;
import com.tsurugidb.iceaxe.transaction.option.TgTxOptionOcc;
import com.tsurugidb.iceaxe.transaction.option.TgTxOptionRtx;
import com.tsurugidb.sql.proto.SqlRequest.TransactionPriority;

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

    TgTxOptionLtx ltx_priority() {
        return TgTxOption.ofLTX("tableName1").priority(TransactionPriority.INTERRUPT_EXCLUDE);
    }

    TgTxOptionLtx ltx_readArea() {
        return TgTxOption.ofLTX("tableName1").addInclusiveReadArea("tableName2");
    }

    TgTxOptionLtx ltx_defaultOption() {
        var defaultOption = TgTxOption.ofLTX().addExclusiveReadArea("test1", "test2");
        return TgTxOption.ofLTX(defaultOption).addWritePreserve("tableName1");
    }

    // READ ONLY
    TgTxOptionRtx rtx() {
        return TgTxOption.ofRTX();
    }

    TgTxOptionRtx rtx_priority() {
        return TgTxOption.ofRTX().priority(TransactionPriority.WAIT);
    }

    // DDL(LTX)
    TgTxOptionLtx ddl() {
        return TgTxOption.ofDDL();
    }

    // label
    TgTxOption label() {
        return TgTxOption.ofOCC().label("transaction label");
    }

    // clone
    TgTxOption cloneSimple(TgTxOption txOption) {
        return txOption.clone();
    }

    TgTxOption cloneWithLabel(TgTxOption txOption) {
        return txOption.clone("new label");
    }
}
