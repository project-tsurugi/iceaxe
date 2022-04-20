package com.example.iceaxe;

import com.tsurugi.iceaxe.transaction.TgTransactionOption;
import com.tsurugi.iceaxe.transaction.TgTransactionType;

/**
 * example for TransationOption
 */
public class TransactionOptionExample {

    public static final TgTransactionOption OCC = TgTransactionOption.of(TgTransactionType.OCC);

    public static final TgTransactionOption BATCH_READ_ONLY = TgTransactionOption.of(TgTransactionType.BATCH_READ_ONLY);
}
