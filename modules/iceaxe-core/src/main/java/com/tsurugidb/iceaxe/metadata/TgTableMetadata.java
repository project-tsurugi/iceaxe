package com.tsurugidb.iceaxe.metadata;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.sql.proto.SqlCommon.Column;
import com.tsurugidb.tsubakuro.sql.TableMetadata;

/**
 * Tsurugi table metadata.
 */
public class TgTableMetadata {

    /** low table metadata */
    protected final TableMetadata lowTableMetadata;

    /**
     * Creates a new instance.
     *
     * @param lowTableMetadata low TableMetadata
     */
    public TgTableMetadata(TableMetadata lowTableMetadata) {
        this.lowTableMetadata = lowTableMetadata;
    }

    /**
     * <em>This method is not yet implemented:</em>
     * get database name.
     *
     * @return database name
     */
    public @Nullable String getDatabaseName() {
        return lowTableMetadata.getDatabaseName().orElse(null);
    }

    /**
     * <em>This method is not yet implemented:</em>
     * get schema name.
     *
     * @return schema name
     */
    public @Nullable String getSchemaName() {
        return lowTableMetadata.getSchemaName().orElse(null);
    }

    /**
     * get table name.
     *
     * @return table name
     */
    public @Nonnull String getTableName() {
        return lowTableMetadata.getTableName();
    }

    /**
     * get column list.
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
