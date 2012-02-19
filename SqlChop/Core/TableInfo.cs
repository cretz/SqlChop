using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace SqlChop.Core
{
    public class TableInfo
    {
        private readonly string _Name;
        private readonly bool _IsSystem;
        private readonly IList<ColumnInfo> _Columns;
        private readonly Dictionary<string, ColumnInfo> _ColumnsByName;

        public TableInfo(string name, bool isSystem, IEnumerable<ColumnInfo> columns)
        {
            _Name = name;
            _IsSystem = isSystem;
            List<ColumnInfo> cols = new List<ColumnInfo>(columns);
            cols.Sort();
            _Columns = cols.AsReadOnly();
            _ColumnsByName = new Dictionary<string, ColumnInfo>(StringComparer.OrdinalIgnoreCase);
            foreach (ColumnInfo column in columns)
            {
                _ColumnsByName[column.Name] = column;
            }
        }

        public string Name { get { return _Name; } }

        public bool IsSystem { get { return _IsSystem; } }

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
