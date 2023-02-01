package com.tsurugidb.iceaxe.example;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOptionSupplier;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * TgTmSetting example.
 *
 * @see Example03TransactionOption
 * @see Example04TransactionManager
 * @see Example92Timeout#timeoutByTmSetting(com.tsurugidb.iceaxe.TsurugiConnector)
 */
@SuppressWarnings("unused")
public class Example04TmSetting {

    void setting1(TgTxOption option) {
        // 指定されたトランザクションオプションでトランザクションを実行
        // トランザクションが失敗したら例外発生
        var setting = TgTmSetting.of(option);
//      var tm = session.createTransactionManager(setting);
    }

    void setting2(TgTxOption... optionList) {
        // 指定されたトランザクションオプションでトランザクションを実行
        // トランザクションがリトライ可能なabortであれば、次に指定されたトランザクションオプションを使って再実行
        // 指定されたトランザクションオプションを全て使ってもトランザクションが失敗した場合は、例外発生
        var setting = TgTmSetting.of(optionList);
//      var tm = session.createTransactionManager(setting);
    }

    void settingAlways(TgTxOption option) {
        // 指定されたトランザクションオプションでトランザクションを実行
        // トランザクションがリトライ可能なabortである限り、指定されたトランザクションオプションを使って再実行し続ける
        var setting = TgTmSetting.ofAlways(option);
//      var tm = session.createTransactionManager(setting);
    }

    void settingAlwaysLimit(TgTxOption option) {
        // 指定されたトランザクションオプションでトランザクションを実行
        // トランザクションがリトライ可能なabortであれば、指定されたトランザクションオプションを使って再実行する
        // 指定された最大試行回数を超えたら、例外発生
        var setting = TgTmSetting.ofAlways(option, 3);
//      var tm = session.createTransactionManager(setting);
    }

    //
    // TgTxOptionSupplier
    //
    void supplier1(TgTxOption option) {
        // same as TgTmSetting.of(option)

        var supplier = TgTmTxOptionSupplier.of(option);
        var setting = TgTmSetting.of(supplier);
//      var tm = session.createTransactionManager(setting);
    }

    void supplier2(TgTxOption... optionList) {
        // same as TgTmSetting.of(optionList)

        var supplier = TgTmTxOptionSupplier.of(optionList);
        var setting = TgTmSetting.of(supplier);
//      var tm = session.createTransactionManager(setting);
    }

    void supplierAlways(TgTxOption option) {
        // same as TgTmSetting.ofAlways(option)

        var supplier = TgTmTxOptionSupplier.ofAlways(option);
        var setting = TgTmSetting.of(supplier);
//      var tm = session.createTransactionManager(setting);
    }

    void supplierAlwaysLimit(TgTxOption option) {
        // same as TgTmSetting.ofAlways(option, 3)

        var supplier = TgTmTxOptionSupplier.ofAlways(option, 3);
        var setting = TgTmSetting.of(supplier);
//      var tm = session.createTransactionManager(setting);
    }

    void supplierCustom(TgTxOption firstOption, TgTxOption laterOption) {
        var supplier = new TgTmTxOptionSupplier() {
            @Override
            protected TgTmTxOption computeFirstTmOption() {
                // 初回はfirstOptionでトランザクション実行
                return TgTmTxOption.execute(firstOption);
            }

            @Override
            protected TgTmTxOption computeRetryTmOption(int attempt, TsurugiTransactionException e) {
                // 2回目以降でリトライ可能な場合はlaterOptionでトランザクション実行
                return TgTmTxOption.execute(laterOption);
            }
        };
        var setting = TgTmSetting.of(supplier);
//      var tm = session.createTransactionManager(setting);
    }

    void supplierLog(TgTxOption option) {
        var supplier = TgTmTxOptionSupplier.ofAlways(option, Integer.MAX_VALUE);
        supplier.setTmOptionListener((attempt, e, tmOption) -> {
            if (attempt > 0 && tmOption.isExecute()) {
                System.out.println("retry " + attempt);
            }
        });
        var setting = TgTmSetting.of(supplier);
    }

    void supplierLogFromSetting(TgTxOption option) {
        var setting = TgTmSetting.ofAlways(option);
        setting.getTransactionOptionSupplier().setTmOptionListener((attempt, e, tmOption) -> {
            if (attempt > 0 && tmOption.isExecute()) {
                System.out.println("retry " + attempt);
            }
        });
    }
}
