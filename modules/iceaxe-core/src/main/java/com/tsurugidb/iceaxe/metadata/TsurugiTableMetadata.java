package com.tsurugidb.iceaxe.metadata;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.sql.proto.SqlCommon.Column;
import com.tsurugidb.tsubakuro.sql.TableMetadata;

/**
 * Tsurugi table metadata
 */
public class TsurugiTableMetadata {

    private final TableMetadata lowTableMetadata;

    // internal
    public TsurugiTableMetadata(TableMetadata lowTableMetadata) {
        this.lowTableMetadata = lowTableMetadata;
    }

    /**
     * get database name
     * 
     * @return database name
     */
    @Nullable
    public String getDatabaseName() {
        return lowTableMetadata.getDatabaseName().orElse(null);
    }

    /**
     * get schema name
     * 
     * @return schema name
     */
    @Nullable
    public String getSchemaName() {
        return lowTableMetadata.getSchemaName().orElse(null);
    }

    /**
     * get table name
     * 
     * @return table name
     */
    @Nonnull
    public String getTableName() {
        return lowTableMetadata.getTableName();
    }

    /**
     * get column list
     * 
     * @return column list
     */
    public List<? extends Column> getLowColumnList() {
        return lowTableMetadata.getColumns();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + getTableName() + "]";
    }
}
