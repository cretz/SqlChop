using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace SqlChop.Core
{
    public class TableInfo
    {
        private readonly string _Catalog;
        private readonly string _Schema;
        private readonly string _Name;
        private readonly bool _IsFound;
        private readonly IList<ColumnInfo> _Columns;
        private readonly Dictionary<string, ColumnInfo> _ColumnsByName;

        internal TableInfo(SqlConnection connection, string tableName) :
            this(connection, null, null, tableName) { }

        internal TableInfo(SqlConnection connection, string catalog, string schema, string tableName)
        {
            DataTable dt = connection.GetSchema(SqlClientMetaDataCollectionNames.Columns,
                new string[] { catalog, schema, tableName, null });
            if (dt.Rows.Count == 0)
            {
                _Catalog = catalog;
                _Schema = schema;
                _Name = tableName;
                _IsFound = false;
                _Columns = new ColumnInfo[0];
                _ColumnsByName = new Dictionary<string, ColumnInfo>(0);
            }
            else
            {
                _Catalog = (string)dt.Rows[0]["TABLE_CATALOG"];
                _Schema = (string)dt.Rows[0]["TABLE_SCHEMA"];
                _Name = (string)dt.Rows[0]["TABLE_NAME"];
                _IsFound = true;
                List<ColumnInfo> cols = new List<ColumnInfo>(dt.Rows.Count);
                _ColumnsByName = new Dictionary<string, ColumnInfo>(dt.Rows.Count);
                foreach (DataRow row in dt.Rows)
                {
                    ColumnInfo col = new ColumnInfo(row);
                    cols.Add(col);
                    _ColumnsByName[col.Name] = col;
                }
                //sort by ordinal, very important
                cols.Sort();
                _Columns = cols.AsReadOnly();
            }
        }
        public string Catalog { get { return _Catalog; } }

        public string Schema { get { return _Schema; } }

        public string Name { get { return _Name; } }

        public bool IsFound { get { return _IsFound; } }

        public ColumnInfo this[int index] { get { return _Columns[index]; } }

        public ColumnInfo this[string name] { get { return _ColumnsByName[name]; } }

        public IList<ColumnInfo> Columns { get { return _Columns; } }

        public ICollection<string> ColumnNames { get { return _ColumnsByName.Keys; } }

        public bool HasColumn(string name)
        {
            return _ColumnsByName.ContainsKey(name);
        }
    }
}
