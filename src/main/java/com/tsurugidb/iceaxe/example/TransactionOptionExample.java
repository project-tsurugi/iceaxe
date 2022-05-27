package com.tsurugidb.iceaxe.example;

import com.tsurugidb.iceaxe.transaction.TgTransactionOption;
import com.tsurugidb.iceaxe.transaction.TgTransactionType;

/**
 * example for TransationOption
 */
public class TransactionOptionExample {

    public static final TgTransactionOption OCC = TgTransactionOption.of(TgTransactionType.OCC);

    public static final TgTransactionOption BATCH_READ_ONLY = TgTransactionOption.of(TgTransactionType.BATCH_READ_ONLY);
}
