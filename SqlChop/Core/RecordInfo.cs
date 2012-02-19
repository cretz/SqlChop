using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;

namespace SqlChop.Core
{
    public class RecordInfo
    {
        internal static RecordInfo BuildRecordInfo(SqlDataReader reader, ITableInfoProvider tableInfoProvider)
        {
            RecordInfo record = new RecordInfo();
            record._Operation = GetOperation((string)reader["Operation"]);
            record._LogSequenceNumber = new LogSequenceNumber(reader);
            if (!reader.IsDBNull(reader.GetOrdinal("AllocUnitName")))
            {
                record._AllocationUnitName = AllocationUnitName.FromString((string)reader["AllocUnitName"]);
                if (record._AllocationUnitName != null)
                {
                    record._Table = tableInfoProvider.GetTable(record._AllocationUnitName.ObjectName,
                        "sys".Equals(record._AllocationUnitName.SchemaName));
                }
            }
            if (!reader.IsDBNull(reader.GetOrdinal("RowLog Contents 0")))
            {
                record._RowLogContents0 = reader.GetSqlBytes(reader.GetOrdinal("RowLog Contents 0")).Buffer;
            }
            if (!reader.IsDBNull(reader.GetOrdinal("RowLog Contents 1")))
            {
                record._RowLogContents1 = reader.GetSqlBytes(reader.GetOrdinal("RowLog Contents 1")).Buffer;
            }
            if (!reader.IsDBNull(reader.GetOrdinal("Offset in Row")))
            {
                record._RowOffset = (short)reader["Offset in Row"];
            }
            return record;
        }

        private static Operation GetOperation(string operation)
        {
            switch (operation)
            {
                case "LOP_BEGIN_XACT": return Operation.BeginTransaction;
                case "LOP_COMMIT_XACT": return Operation.CommitTransaction;
                case "LOP_INSERT_ROWS": return Operation.InsertRows;
                case "LOP_MODIFY_ROW": return Operation.ModifyRow;
                case "LOP_MODIFY_COLUMNS": return Operation.ModifyColumns;
                case "LOP_DELETE_ROWS": return Operation.DeleteRows;
                default: return Operation.Unknown;
            }
        }

        private Operation _Operation;
        private LogSequenceNumber _LogSequenceNumber;
        private AllocationUnitName _AllocationUnitName;
        private TableInfo _Table;
        private byte[] _RowLogContents0;
        private byte[] _RowLogContents1;
        private int? _RowOffset;
        private RowInfo _OriginalRow;
        private RowInfo _NewRow;

        private RecordInfo() { }

        public Operation Operation { get { return _Operation; } }

        public LogSequenceNumber LogSequenceNumber { get { return _LogSequenceNumber; } }

        public TableInfo Table { get { return _Table; } }

        public RowInfo OriginalRow
        {
            get
            {
                if (_Table != null && _RowLogContents0 != null && _OriginalRow == null && (_Operation ==
                    global::SqlChop.Core.Operation.DeleteRows || _Operation == global::SqlChop.Core.Operation.ModifyRow))
                {
                    _OriginalRow = new RowInfo(_RowLogContents0, Table, _Operation, _RowOffset);
                }
                return _OriginalRow;
            }
        }

        public RowInfo NewRow
        {
            get
            {
                if (_NewRow == null && _Table != null)
                {
                    if (_Operation == global::SqlChop.Core.Operation.InsertRows && _RowLogContents0 != null)
                    {
                        _NewRow = new RowInfo(_RowLogContents0, Table, _Operation, _RowOffset);
                    }
                    else if (_Operation == global::SqlChop.Core.Operation.ModifyRow && _RowLogContents1 != null)
                    {
                        _NewRow = new RowInfo(_RowLogContents1, Table, _Operation, _RowOffset);
                    }
                }
                return _NewRow;
            }
        }
    }
}
