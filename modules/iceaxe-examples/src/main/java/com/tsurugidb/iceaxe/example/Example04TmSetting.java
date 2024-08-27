/*
 * Copyright 2023-2024 Project Tsurugi.
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

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOptionSupplier;
import com.tsurugidb.iceaxe.transaction.manager.retry.TgTmRetryInstruction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * TgTmSetting example.
 *
 * @see Example03TxOption
 * @see Example04TransactionManager
 * @see Example92Timeout#timeoutByTmSetting(com.tsurugidb.iceaxe.TsurugiConnector)
 */
@SuppressWarnings("unused")
public class Example04TmSetting {

    void setting1(TgTxOption txOption) {
        // 指定されたトランザクションオプションでトランザクションを実行
        // トランザクションが失敗したら例外発生
        var setting = TgTmSetting.of(txOption);
//      var tm = session.createTransactionManager(setting);
    }

    void setting2(TgTxOption... txOptions) {
        // 指定されたトランザクションオプションでトランザクションを実行
        // トランザクションがリトライ可能なabortであれば、次に指定されたトランザクションオプションを使って再実行
        // 指定されたトランザクションオプションを全て使ってもトランザクションが失敗した場合は、例外発生
        var setting = TgTmSetting.of(txOptions);
//      var tm = session.createTransactionManager(setting);
    }

    void settingAlways(TgTxOption txOption) {
        // 指定されたトランザクションオプションでトランザクションを実行
        // トランザクションがリトライ可能なabortである限り、指定されたトランザクションオプションを使って再実行し続ける
        var setting = TgTmSetting.ofAlways(txOption);
//      var tm = session.createTransactionManager(setting);
    }

    void settingAlwaysLimit(TgTxOption txOption) {
        // 指定されたトランザクションオプションでトランザクションを実行
        // トランザクションがリトライ可能なabortであれば、指定されたトランザクションオプションを使って再実行する
        // 指定された最大試行回数を超えたら、例外発生
        var setting = TgTmSetting.ofAlways(txOption, 3);
//      var tm = session.createTransactionManager(setting);
    }

    void settingMultiple1(TgTxOption txOption) {
        var setting = TgTmSetting.of(txOption, 3);
//      var tm = session.createTransactionManager(setting);
    }

    void settingMultiple2(TgTxOption option1, TgTxOption option2) {
        var setting = TgTmSetting.of(option1, 3, option2, 1);
//      var tm = session.createTransactionManager(setting);
    }

    //
    // TgTmTxOptionSupplier
    //
    void supplier1(TgTxOption txOption) {
        // same as TgTmSetting.of(txOption)

        var supplier = TgTmTxOptionSupplier.of(txOption);
        var setting = TgTmSetting.of(supplier);
//      var tm = session.createTransactionManager(setting);
    }

    void supplier2(TgTxOption... txOptions) {
        // same as TgTmSetting.of(txOptions)

        var supplier = TgTmTxOptionSupplier.of(txOptions);
        var setting = TgTmSetting.of(supplier);
//      var tm = session.createTransactionManager(setting);
    }

    void supplierAlways(TgTxOption txOption) {
        // same as TgTmSetting.ofAlways(txOption)

        var supplier = TgTmTxOptionSupplier.ofAlways(txOption);
        var setting = TgTmSetting.of(supplier);
//      var tm = session.createTransactionManager(setting);
    }

    void supplierAlwaysLimit(TgTxOption txOption) {
        // same as TgTmSetting.ofAlways(txOption, 3)

        var supplier = TgTmTxOptionSupplier.ofAlways(txOption, 3);
        var setting = TgTmSetting.of(supplier);
//      var tm = session.createTransactionManager(setting);
    }

    void supplierMultiple1(TgTxOption txOption) {
        // same as TgTmSetting.of(txOption, 3)

        var supplier = TgTmTxOptionSupplier.of(txOption, 3);
        var setting = TgTmSetting.of(supplier);
//      var tm = session.createTransactionManager(setting);
    }

    void supplierMultiple2(TgTxOption txOption1, TgTxOption txOption2) {
        // same as TgTmSetting.of(txOption1, 3, txOption2, 1)

        var supplier = TgTmTxOptionSupplier.of(txOption1, 3, txOption2, 1);
        var setting = TgTmSetting.of(supplier);
//      var tm = session.createTransactionManager(setting);
    }

    void supplierCustom(TgTxOption firstTxOption, TgTxOption laterTxOption) {
        var supplier = new TgTmTxOptionSupplier() {
            @Override
            protected TgTmTxOption computeFirstTmOption(Object executeInfo) {
                // 初回はfirstOptionでトランザクション実行
                return TgTmTxOption.execute(firstTxOption, null);
            }

            @Override
            protected TgTmTxOption computeRetryTmOption(Object executeInfo, int attempt, TsurugiTransactionException exception, TgTmRetryInstruction retryInstruction) {
                // 2回目以降でリトライ可能な場合はlaterOptionでトランザクション実行
                return TgTmTxOption.execute(laterTxOption, retryInstruction);
            }
        };
        var setting = TgTmSetting.of(supplier);
//      var tm = session.createTransactionManager(setting);
    }

    void supplierLog(TgTxOption txOption) {
        var supplier = TgTmTxOptionSupplier.ofAlways(txOption, Integer.MAX_VALUE);
        supplier.setTmOptionListener((attempt, exception, tmOption) -> {
            if (attempt > 0 && tmOption.isExecute()) {
                System.out.println("retry " + attempt);
            }
        });
        var setting = TgTmSetting.of(supplier);
    }

    void supplierLogFromSetting(TgTxOption txOption) {
        var setting = TgTmSetting.ofAlways(txOption);
        setting.getTransactionOptionSupplier().setTmOptionListener((attempt, exception, tmOption) -> {
            if (attempt > 0 && tmOption.isExecute()) {
                System.out.println("retry " + attempt);
            }
        });
    }
}
