package com.tsurugidb.iceaxe.transaction.option;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Collectors;

import com.tsurugidb.sql.proto.SqlRequest.TransactionPriority;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;

public class TgTxOptionTester {

    protected final TransactionType expectedType;

    public TgTxOptionTester(TransactionType type) {
        this.expectedType = type;
    }

    protected void assertLowOption(String label, TransactionPriority priority, List<String> writePreserve, List<String> inclusiveReadArea, List<String> exclusiveReadArea, TgTxOption txOption) {
        var builder = txOption.toLowTransactionOption();
        assertEquals(expectedType, builder.getType());
        assertEquals((label != null) ? label : "", builder.getLabel());
        assertEquals((priority != null) ? priority : TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED, builder.getPriority());
        assertEquals(writePreserve, builder.getWritePreservesList().stream().map(wp -> wp.getTableName()).collect(Collectors.toList()));
        assertEquals(inclusiveReadArea, builder.getInclusiveReadAreasList().stream().map(ra -> ra.getTableName()).collect(Collectors.toList()));
        assertEquals(exclusiveReadArea, builder.getExclusiveReadAreasList().stream().map(ra -> ra.getTableName()).collect(Collectors.toList()));
    }
}
