// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.jdbc;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.SchemaConstants;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Math.max;

public class OracleSchemaResolver extends JDBCSchemaResolver {

    @Override
    public ResultSet getTables(Connection connection, String dbName) throws SQLException {
        return connection.getMetaData().getTables(connection.getCatalog(), dbName, null,
                new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"});
    }

    @Override
    public ResultSet getColumns(Connection connection, String dbName, String tblName) throws SQLException {
        return connection.getMetaData().getColumns(connection.getCatalog(), dbName, tblName, "%");
    }

    @Override
    public List<Column> convertToSRTable(ResultSet columnSet) throws SQLException {
        List<Column> fullSchema = Lists.newArrayList();
        while (columnSet.next()) {
            Type type = convertColumnType(columnSet.getInt("DATA_TYPE"),
                    columnSet.getString("TYPE_NAME"),
                    columnSet.getInt("COLUMN_SIZE"),
                    columnSet.getInt("DECIMAL_DIGITS"));
            String columnName = columnSet.getString("COLUMN_NAME");
            fullSchema.add(new Column(columnName, type,
                    columnSet.getString("IS_NULLABLE").equals(SchemaConstants.YES)));
        }
        return fullSchema;
    }

    @Override
    public Table getTable(long id, String name, List<Column> schema, String dbName, String catalogName,
                          Map<String, String> properties) throws DdlException {
        Map<String, String> newProp = new HashMap<>(properties);
        newProp.putIfAbsent(JDBCTable.JDBC_TABLENAME, "\"" + dbName + "\"" + "." + "\"" + name + "\"");
        return new JDBCTable(id, name, schema, dbName, catalogName, newProp);
    }

    @Override
    public Table getTable(long id, String name, List<Column> schema, List<Column> partitionColumns, String dbName,
                          String catalogName, Map<String, String> properties) throws DdlException {
        Map<String, String> newProp = new HashMap<>(properties);
        newProp.putIfAbsent(JDBCTable.JDBC_TABLENAME, "\"" + dbName + "\"" + "." + "\"" + name + "\"");
        return new JDBCTable(id, name, schema, partitionColumns, dbName, catalogName, newProp);
    }

    @Override
    public Type convertColumnType(int dataType, String typeName, int columnSize, int digits) {
        PrimitiveType primitiveType;
        switch (dataType) {
            case Types.SMALLINT:
                primitiveType = PrimitiveType.SMALLINT;
                break;
            case Types.FLOAT:
            // BINARY_FLOAT
            case 100:
                primitiveType = PrimitiveType.FLOAT;
                break;
            case Types.DOUBLE:
            // BINARY_DOUBLE
            case 101:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case Types.NUMERIC:
            // NUMBER
            case 3:
                primitiveType = PrimitiveType.DECIMAL32;
                break;
            case Types.CHAR:
            case Types.NCHAR:
                return ScalarType.createCharType(columnSize);
            case Types.VARCHAR:
            // NVARCHAR2
            case Types.NVARCHAR:
                if (columnSize > 0) {
                    return ScalarType.createVarcharType(columnSize);
                } else {
                    return ScalarType.createVarcharType(ScalarType.CATALOG_MAX_VARCHAR_LENGTH);
                }
            case Types.CLOB:
            case Types.NCLOB:
            // LONG
            case Types.LONGVARCHAR:
                return ScalarType.createVarcharType(ScalarType.CATALOG_MAX_VARCHAR_LENGTH);
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            // raw
            case 23:
                if (columnSize > 0) {
                    return ScalarType.createVarbinary(columnSize);
                } else {
                    return ScalarType.createVarbinary(ScalarType.CATALOG_MAX_VARCHAR_LENGTH);
                }
            case Types.DATE:
                primitiveType = PrimitiveType.DATE;
                break;
            // Don't support timestamp type, just convert it to string
            case Types.TIMESTAMP:
            // TIMESTAMP WITH LOCAL TIME ZONE
            case -102:
            // TIMESTAMP WITH TIME ZONE
            case -101:
                return ScalarType.createVarcharType(ScalarType.CATALOG_MAX_VARCHAR_LENGTH);
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }

        if (primitiveType != PrimitiveType.DECIMAL32) {
            return ScalarType.createType(primitiveType);
        } else {
            int precision = columnSize + max(-digits, 0);
            // if user not specify numeric precision and scale, the default value is 0,
            // we can't defer the precision and scale, can only deal it as string.
            if (precision == 0) {
                return ScalarType.createVarcharType(ScalarType.CATALOG_MAX_VARCHAR_LENGTH);
            }
            return ScalarType.createUnifiedDecimalType(precision, max(digits, 0));
        }
    }
}