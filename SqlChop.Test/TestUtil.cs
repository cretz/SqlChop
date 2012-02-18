using System;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using System.Data.SqlClient;
using System.Data;
using SqlChop.Core;

namespace SqlChop.Test
{
    [SetUpFixture]
    public class TestUtils
    {
        public const string DATABASE_NAME = "DbTest";

        private static SqlConnection _Connection;

        [SetUp]
        public static void Init()
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = "localhost,8081";
            builder.InitialCatalog = DATABASE_NAME;
            builder.UserID = "sa";
            builder.Password = "sapass";
            builder.MultipleActiveResultSets = true;
            Console.WriteLine("Opening connection: {0}", builder.ConnectionString);
            _Connection = new SqlConnection(builder.ConnectionString);
            _Connection.Open();
        }

        internal static SqlConnection Connection { get { return _Connection; } }

        internal static void ExecuteNonQuery(string sql)
        {
            new SqlCommand(sql, _Connection).ExecuteNonQuery();
        }

        internal static void DropTableIfExists(string tableName)
        {
            ExecuteNonQuery("IF OBJECT_ID('" + tableName + "', 'U') IS NOT NULL\nDROP TABLE " + tableName);
        }

        internal static void CreateDataTypesTable(string tableName, ICollection<SqlDataType> types, bool nullable)
        {
            //drop if there
            DropTableIfExists(tableName);
            //build query
            StringBuilder cols = new StringBuilder();
            foreach (SqlDataType type in types)
            {
                if (cols.Length > 0)
                {
                    cols.Append(',');
                }
                cols.Append(Enum.GetName(typeof(SqlDbType), type.Type)).Append("Col ").
                    Append(Enum.GetName(typeof(SqlDbType), type.Type));
                switch (type.Type)
                {
                    case SqlDbType.Binary:
                    case SqlDbType.Char:
                    case SqlDbType.NChar:
                    case SqlDbType.NVarChar:
                    case SqlDbType.VarBinary:
                    case SqlDbType.VarChar:
                        cols.Append('(').Append(type.Length.Value).Append(')');
                        break;
                    case SqlDbType.Decimal:
                        cols.Append('(').Append(type.Precision.Value).Append(',').
                            Append(type.Scale.Value).Append(')');
                        break;
                }
                cols.Append(' ');
                if (!nullable)
                {
                    cols.Append("NOT ");
                }
                cols.Append("NULL");
            }
            //create
            ExecuteNonQuery("CREATE TABLE " + tableName + " (" + cols + ")");
        }

        internal static void InsertIntoDataTypesTable(string tableName, Dictionary<SqlDataType, object> vals)
        {
            StringBuilder cols = new StringBuilder();
            StringBuilder values = new StringBuilder();
            SqlCommand cmd = new SqlCommand();
            foreach (KeyValuePair<SqlDataType, object> val in vals)
            {
                if (cols.Length > 0)
                {
                    cols.Append(',');
                    values.Append(',');
                }
                cols.Append(Enum.GetName(typeof(SqlDbType), val.Key.Type)).Append("Col");
                string paramName = '@' + Enum.GetName(typeof(SqlDbType), val.Key.Type) + "Val";
                values.Append(paramName);
                cmd.Parameters.Add(paramName, val.Key.Type);
                cmd.Parameters[paramName].Value = val.Value == null ? DBNull.Value : val.Value;
            }
            cmd.CommandText = "INSERT INTO " + tableName + " (" + cols + ") VALUES (" + values + ")";
            cmd.Connection = _Connection;
            Assert.AreEqual(1, cmd.ExecuteNonQuery());
        }

        internal static void UpdateDataTypesTable(string tableName, Dictionary<SqlDataType, object> vals)
        {
            StringBuilder cols = new StringBuilder();
            SqlCommand cmd = new SqlCommand();
            foreach (KeyValuePair<SqlDataType, object> val in vals)
            {
                if (cols.Length > 0)
                {
                    cols.Append(',');
                }
                string paramName = '@' + Enum.GetName(typeof(SqlDbType), val.Key.Type) + "Val";
                cols.Append(Enum.GetName(typeof(SqlDbType), val.Key.Type)).Append("Col = ").Append(paramName);
                cmd.Parameters.Add(paramName, val.Key.Type);
                cmd.Parameters[paramName].Value = val.Value == null ? DBNull.Value : val.Value;
            }
            cmd.CommandText = "UPDATE " + tableName + " SET " + cols;
            cmd.Connection = _Connection;
            Assert.AreEqual(1, cmd.ExecuteNonQuery());
        }

        internal static void DeleteFromDataTypesTable(string tableName)
        {
            new SqlCommand("DELETE FROM " + tableName, _Connection).ExecuteScalar();
        }

        internal static LogSequenceNumber GetLastTransactionBeginLsn()
        {
            return new LogSequenceNumber((string)new SqlCommand(@"
SELECT TOP 1 [Current LSN] FROM fn_dblog(null, null)
WHERE Operation = 'LOP_BEGIN_XACT'
ORDER BY [Current LSN] DESC", _Connection).ExecuteScalar());
        }

        internal static RecordInfo GetLatestRecord(string tableName, Operation operation)
        {
            //grab the latest LSN
            LogSequenceNumber latestLsn = TestUtils.GetLastTransactionBeginLsn();
            //start log reader
            LogReader reader = new LogReader(TestUtils.Connection);
            List<RecordInfo> records = new List<RecordInfo>();
            reader.RecordReceived += delegate(RecordInfo record)
            {
                if (record.Operation == operation && record.Table != null &&
                    tableName.Equals(record.Table.Name))
                {
                    records.Add(record);
                }
            };
            reader.Poll(latestLsn);
            Assert.AreEqual(1, records.Count);
            return records[0];
        }

        [TearDown]
        public static void Cleanup()
        {
            if (Connection.State == ConnectionState.Open)
            {
                Console.WriteLine("Closing connection");
                Connection.Close();
            }
        }
    }
}
