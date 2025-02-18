/*
 * Copyright 2023-2025 Project Tsurugi.
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

import java.io.IOException;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * TsurugiTransactionManager example
 *
 * <p>
 * TransactionManagerのexecuteメソッドは、ひとつのトランザクションを実行する。<br>
 * 実行が正常に終了した場合はコミットし、例外が発生した場合はロールバックする。<br>
 * トランザクション内でリトライ可能なアボートが発生した場合は、自動的にトランザクションの先頭から再実行を行う。
 * </p>
 *
 * @see Example04TmSetting
 */
public class Example04TransactionManager {

    private static final TgTxOption OCC = TgTxOption.ofOCC();
    private static final TgTxOption LTX = TgTxOption.ofLTX("table1", "table2");

    /**
     * @see Example04TmSetting
     */
    private static final TgTmSetting SETTING = TgTmSetting.of(OCC, LTX);

    public static void main(String... args) throws IOException, InterruptedException {
        try (var session = Example02Session.createSession()) {
            new Example04TransactionManager().main(session);
        }
    }

    void main(TsurugiSession session) throws IOException, InterruptedException {
        manager1(session);
        manager2(session);
    }

    void manager1(TsurugiSession session) throws IOException, InterruptedException {
        var tm = session.createTransactionManager(SETTING);
        tm.execute(transaction -> {
//          transaction.executeAndXxx(ps);
        });
    }

    void manager2(TsurugiSession session) throws IOException, InterruptedException {
        var tm = session.createTransactionManager();
        tm.execute(SETTING, transaction -> {
//          transaction.executeAndXxx(ps);
        });
    }
}
