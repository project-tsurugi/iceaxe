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
package com.tsurugidb.iceaxe.metadata;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.util.IceaxeInternal;
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
     * get description.
     *
     * @return description
     * @since 1.9.0
     */
    public @Nullable String getDescription() {
        return lowTableMetadata.getDescription().orElse(null);
    }

    /**
     * get column list.
     *
     * @return column list
     * @since 1.9.0
     */
    public List<TgSqlColumn> getColumnList() {
        return lowTableMetadata.getColumns().stream().map(TgSqlColumn::new).collect(Collectors.toList());
    }

    /**
     * get low column list.
     *
     * @return column list
     */
    @IceaxeInternal
    public List<? extends Column> getLowColumnList() {
        return lowTableMetadata.getColumns();
    }

    /**
     * get primary keys.
     *
     * @return primary key column name list
     * @since 1.10.0
     */
    public List<String> getPrimaryKeys() {
        return lowTableMetadata.getPrimaryKeys();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + getTableName() + "]";
    }
}
