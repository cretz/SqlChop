using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using System.Data.SqlTypes;

namespace SqlChop.Core
{
    public delegate void RecordEventHandler(RecordInfo record);

    public class LogReader
    {
        public event RecordEventHandler RecordReceived;

        private readonly SqlConnection _Connection;
        private SqlCommand _Command;
        private readonly TableInfoCache _TableInfoCache;

        public LogReader(SqlConnection connection)
        {
            _Connection = connection;
            _TableInfoCache = new TableInfoCache(connection);
        }

        public void Poll(LogSequenceNumber begin)
        {
            Poll(begin, null);
        }

        public void Poll(LogSequenceNumber begin, LogSequenceNumber end)
        {
            if (_Command == null)
            {
                _Command = new SqlCommand("SELECT * FROM fn_dblog(@Begin, @End) ORDER BY [Current LSN] DESC", _Connection);
                _Command.Parameters.Add("@Begin", SqlDbType.VarChar);
                _Command.Parameters.Add("@End", SqlDbType.VarChar);
            }
            _Command.Parameters["@Begin"].Value = begin == null ? (object)DBNull.Value : begin.ToString();
            _Command.Parameters["@End"].Value = end == null ? (object)DBNull.Value : end.ToString();
            SqlDataReader reader = _Command.ExecuteReader();
            while (reader.Read())
            {
                RecordInfo record = new RecordInfo(reader, _TableInfoCache);
                if (RecordReceived != null)
                {
                    RecordReceived(record);
                }
            }
            reader.Close();
        }

        private byte[] GetByteArrayColumn(SqlDataReader reader, string column)
        {
            SqlBytes bytes = reader.GetSqlBytes(reader.GetOrdinal(column));
            return bytes.Buffer;
        }
    }
}
