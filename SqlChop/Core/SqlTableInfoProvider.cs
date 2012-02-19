using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace SqlChop.Core
{
    public class SqlTableInfoProvider : ITableInfoProvider
    {
        private readonly SqlConnection _Connection;
        private readonly Dictionary<string, TableInfo> _Tables = new Dictionary<string, TableInfo>(
            StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, bool> _TablesNotFound = new Dictionary<string, bool>(
            StringComparer.OrdinalIgnoreCase);

        public SqlTableInfoProvider(SqlConnection connection)
        {
            _Connection = connection;
        }

        public TableInfo GetTable(string name, bool system)
        {
            //TODO: should I block here?
            lock (_Tables)
            {
                TableInfo table;
                if (_Tables.TryGetValue(name, out table))
                {
                    return table;
                }
                else if (_TablesNotFound.ContainsKey(name))
                {
                    return null;
                }
                else
                {
                    table = LoadTable(name, system);
                    if (table == null)
                    {
                        _TablesNotFound[name] = true;
                    }
                    else
                    {
                        _Tables[name] = table;
                    }
                    return table;
                }
            }
        }

        private TableInfo LoadTable(string name, bool system)
        {
            SqlCommand cmd = new SqlCommand(@"SELECT AC.name AS Name,
  AC.column_id AS OrdinalPosition,
  T.name AS DataType,
  AC.max_length AS MaxByteLength,
  AC.precision AS Precision,
  AC.scale AS Scale,
  AC.is_nullable AS IsNullable
FROM sys.sysobjects SO (NOLOCK)
  JOIN sys.all_columns AC (NOLOCK)
    ON AC.object_id = SO.id
  JOIN sys.types T (NOLOCK)
    ON T.system_type_id = AC.system_type_id
WHERE SO.xtype = @XType
  AND SO.name = @TableName
  AND T.system_type_id = T.user_type_id");
            cmd.Parameters.AddWithValue("@XType", system ? 'S' : 'U');
            cmd.Parameters.AddWithValue("@TableName", name);
            cmd.Connection = _Connection;
            SqlDataReader reader = cmd.ExecuteReader();
            try
            {
                List<ColumnInfo> cols = new List<ColumnInfo>();
                while (reader.Read())
                {
                    ColumnInfo col = new ColumnInfo();
                    col.Name = (string)reader["Name"];
                    string type = (string)reader["DataType"];
                    if ("numeric".Equals(type))
                    {
                        type = "decimal";
                    }
                    col.DataType = (SqlDbType)Enum.Parse(typeof(SqlDbType), type, true);
                    col.IsNullable = reader.GetBoolean(reader.GetOrdinal("IsNullable"));
                    col.OrdinalPosition = (int)reader["OrdinalPosition"];
                    col.MaxByteLength = (int)(short)reader["MaxByteLength"];
                    switch (col.DataType)
                    {
                        case SqlDbType.Char:
                        case SqlDbType.VarChar:
                            col.MaxCharacterLength = col.MaxByteLength;
                            break;
                        case SqlDbType.NChar:
                        case SqlDbType.NVarChar:
                            col.MaxCharacterLength = col.MaxByteLength / 2;
                            break;
                        case SqlDbType.Decimal:
                            col.Precision = (byte)reader["Precision"];
                            col.Scale = (byte)reader["Scale"];
                            break;
                    }
                    cols.Add(col);
                }
                return new TableInfo(name, system, cols);
            }
            finally
            {
                try { reader.Close(); }
                catch { }
            }
        }

        public void InvalidateAllTables()
        {
            lock (_Tables)
            {
                _Tables.Clear();
                _TablesNotFound.Clear();
            }
        }

        public void InvalidateTable(string name)
        {
            lock (_Tables)
            {
                _Tables.Remove(name);
                _TablesNotFound.Remove(name);
            }
        }
    }
}
