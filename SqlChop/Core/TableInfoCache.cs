using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;

namespace SqlChop.Core
{
    internal class TableInfoCache
    {
        private readonly Dictionary<string, TableInfo> _TableCache = new Dictionary<string, TableInfo>();
        private readonly SqlConnection _Connection;

        internal TableInfoCache(SqlConnection connection)
        {
            _Connection = connection;
        }

        public TableInfo GetTableInfo(SqlDataReader reader)
        {
            return GetTableInfo((string)reader["AllocUnitName"]);
        }

        public TableInfo GetTableInfo(string allocUnitName)
        {
            int dotIndex = allocUnitName.IndexOf('.');
            if (dotIndex == -1)
            {
                return null;
            }
            string schema = allocUnitName.Substring(0, dotIndex);
            int endIndex = allocUnitName.IndexOf('.', dotIndex + 1);
            string tableName;
            if (endIndex == -1)
            {
                tableName = allocUnitName.Substring(dotIndex + 1);
            }
            else
            {
                tableName = allocUnitName.Substring(dotIndex + 1, endIndex);
            }
            //is it in the cache?
            string qualifiedName = schema + '.' + tableName;
            if (!_TableCache.ContainsKey(qualifiedName))
            {
                _TableCache[qualifiedName] = new TableInfo(_Connection, null, schema, tableName);
            }
            return _TableCache[qualifiedName];
        }
    }
}
