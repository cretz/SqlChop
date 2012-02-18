using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;

namespace SqlChop.Core
{
    public class RecordInfo
    {
        internal static Operation GetOperation(SqlDataReader reader)
        {
            switch ((string)reader["Operation"])
            {
                case "LOP_BEGIN_XACT": return Operation.BeginTransaction;
                case "LOP_COMMIT_XACT": return Operation.CommitTransaction;
                case "LOP_INSERT_ROWS": return Operation.Insert;
                case "LOP_MODIFY_ROW": return Operation.Update;
                case "LOP_DELETE_ROWS": return Operation.Delete;
                default: return Operation.Unknown;
            }
        }

        private readonly Operation _Operation;
        private readonly LogSequenceNumber _LogSequenceNumber;
        private readonly TableInfo _Table;
        private readonly byte[] _RowLogContents0;
        private readonly byte[] _RowLogContents1;
        private readonly int? _RowOffset;
        private RowInfo _OriginalRow;
        private RowInfo _NewRow;

        internal RecordInfo(SqlDataReader reader, TableInfoCache cache)
        {
            _Operation = GetOperation(reader);
            _LogSequenceNumber = new LogSequenceNumber(reader);
            if (_Operation == global::SqlChop.Core.Operation.Insert ||
                    _Operation == global::SqlChop.Core.Operation.Update ||
                    _Operation == global::SqlChop.Core.Operation.Delete)
            {
                string allocUnitName = (string)reader["AllocUnitName"];
                if (allocUnitName != null && !"Unknown Alloc Unit".Equals(allocUnitName))
                {
                    _Table = cache.GetTableInfo(allocUnitName);
                    if (_Table != null)
                    {
                        _RowLogContents0 = reader.GetSqlBytes(reader.GetOrdinal("RowLog Contents 0")).Buffer;
                        if (_Operation == global::SqlChop.Core.Operation.Update)
                        {
                            _RowLogContents1 = reader.GetSqlBytes(reader.GetOrdinal("RowLog Contents 1")).Buffer;
                            _RowOffset = (short)reader["Offset in Row"];
                        }
                    }
                }
            }
        }

        public Operation Operation { get { return _Operation; } }

        public LogSequenceNumber LogSequenceNumber { get { return _LogSequenceNumber; } }

        public TableInfo Table { get { return _Table; } }

        public RowInfo OriginalRow
        {
            get
            {
                if (_RowLogContents0 != null && _OriginalRow == null && (_Operation ==
                    global::SqlChop.Core.Operation.Delete || _Operation == global::SqlChop.Core.Operation.Update))
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
                if (_NewRow == null)
                {
                    if (_Operation == global::SqlChop.Core.Operation.Insert && _RowLogContents0 != null)
                    {
                        _NewRow = new RowInfo(_RowLogContents0, Table, _Operation, _RowOffset);
                    }
                    else if (_Operation == global::SqlChop.Core.Operation.Update && _RowLogContents1 != null)
                    {
                        _NewRow = new RowInfo(_RowLogContents1, Table, _Operation, _RowOffset);
                    }
                }
                return _NewRow;
            }
        }
    }
}
