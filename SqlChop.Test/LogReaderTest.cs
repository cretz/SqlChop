using System;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using System.Data;
using System.Data.SqlTypes;
using SqlChop.Core;

namespace SqlChop.Test
{
    [TestFixture]
    public class LogReaderTest
    {
        [Test]
        public void TestBit()
        {
            TestSimpleValues(SqlDbType.Bit, true, false, true);
            TestSimpleValues(SqlDbType.Bit, false, true, true);
            TestSimpleValues(SqlDbType.Bit, true, false, false);
            TestSimpleValues(SqlDbType.Bit, null, true, true);
            TestSimpleValues(SqlDbType.Bit, true, null, true);
        }

        [Test]
        public void TestTinyInt()
        {
            TestSimpleValues(SqlDbType.TinyInt, 0, 255, true);
            TestSimpleValues(SqlDbType.TinyInt, 255, 0, false);
            TestSimpleValues(SqlDbType.TinyInt, 1, null, true);
            TestSimpleValues(SqlDbType.TinyInt, null, 1, true);
        }

        [Test]
        public void TestSmallInt()
        {
            TestSimpleValues(SqlDbType.SmallInt, -32768, 32767, true);
            TestSimpleValues(SqlDbType.SmallInt, 32767, -32768, false);
            TestSimpleValues(SqlDbType.SmallInt, null, 35, true);
            TestSimpleValues(SqlDbType.SmallInt, 47, null, true);
        }

        [Test]
        public void TestInt()
        {
            TestSimpleValues(SqlDbType.Int, -2147483648, 2147483647, true);
            TestSimpleValues(SqlDbType.Int, 2147483647, -2147483648, false);
            TestSimpleValues(SqlDbType.Int, null, 35, true);
            TestSimpleValues(SqlDbType.Int, 47, null, true);
        }

        [Test]
        public void TestBigInt()
        {
            TestSimpleValues(SqlDbType.BigInt, -9223372036854775808, 9223372036854775807, true);
            TestSimpleValues(SqlDbType.BigInt, 9223372036854775807, -9223372036854775808, false);
            TestSimpleValues(SqlDbType.BigInt, null, 35, true);
            TestSimpleValues(SqlDbType.BigInt, 47, null, true);
        }

        [Test]
        public void TestFloat()
        {
            TestSimpleValues(SqlDbType.Float, (double)-1.79e+308, (double)-2.23e-308, true);
            TestSimpleValues(SqlDbType.Float, (double)-2.23e-308, (double)-1.79e+308, false);
            TestSimpleValues(SqlDbType.Float, (double)-2.23e-308, (double)0, true);
            TestSimpleValues(SqlDbType.Float, (double)0, (double)-2.23e-308, false);
            TestSimpleValues(SqlDbType.Float, (double)2.23e-308, (double)1.79e+308, true);
            TestSimpleValues(SqlDbType.Float, (double)1.79e+308, (double)2.23e-308, false);
            TestSimpleValues(SqlDbType.Float, null, (double)12.5, true);
            TestSimpleValues(SqlDbType.Float, (double)8.5, null, true);
        }

        [Test]
        public void TestReal()
        {
            TestSimpleValues(SqlDbType.Real, (float)-3.40e+38, (float)-1.18e-38, true);
            TestSimpleValues(SqlDbType.Real, (float)-1.18e-38, (float)-3.40e+38, false);
            TestSimpleValues(SqlDbType.Real, (float)-1.18e-38, (float)0, true);
            TestSimpleValues(SqlDbType.Real, (float)0, (float)-1.18e-38, false);
            TestSimpleValues(SqlDbType.Real, (float)1.18e-38, (float)3.40e+38, true);
            TestSimpleValues(SqlDbType.Real, (float)3.40e+38, (float)1.18e-38, false);
            TestSimpleValues(SqlDbType.Real, null, (float)12.5, true);
            TestSimpleValues(SqlDbType.Real, (float)8.5, null, true);
        }

        [Test]
        public void TestSmallDateTime()
        {
            TestSimpleValues(SqlDbType.SmallDateTime,
                new DateTime(1900, 1, 1), new DateTime(2079, 6, 6, 23, 59, 00), true);
            TestSimpleValues(SqlDbType.SmallDateTime,
                new DateTime(2079, 6, 6, 23, 59, 00), new DateTime(1900, 1, 1), false);
            TestSimpleValues(SqlDbType.SmallDateTime, null, new DateTime(2012, 12, 12), true);
            TestSimpleValues(SqlDbType.SmallDateTime, new DateTime(2012, 12, 12), null, true);
        }

        [Test]
        public void TestDateTime()
        {
            TestSimpleValues(SqlDbType.DateTime, new DateTime(1973, 1, 1), new DateTime(9999, 12, 31, 23, 59, 59, 997), true);
            TestSimpleValues(SqlDbType.DateTime, new DateTime(9999, 12, 31, 23, 59, 59, 997), new DateTime(1973, 1, 1), false);
            TestSimpleValues(SqlDbType.DateTime, null, new DateTime(2012, 12, 12), true);
            TestSimpleValues(SqlDbType.DateTime, new DateTime(2012, 12, 12), null, true);
        }

        [Test]
        public void TestSmallMoney()
        {
            TestSimpleValues(SqlDbType.SmallMoney, -214748.3648m, 214748.3647m, true);
            TestSimpleValues(SqlDbType.SmallMoney, 214748.3647m, -214748.3648m, false);
            TestSimpleValues(SqlDbType.SmallMoney, null, 999.999m, true);
            TestSimpleValues(SqlDbType.SmallMoney, 999.999m, null, true);
        }

        [Test]
        public void TestMoney()
        {
            TestSimpleValues(SqlDbType.Money, -922337203685477.5808m, 922337203685477.5807m, true);
            TestSimpleValues(SqlDbType.Money, 922337203685477.5807m, -922337203685477.5808m, false);
            TestSimpleValues(SqlDbType.Money, null, 999.999m, true);
            TestSimpleValues(SqlDbType.Money, 999.999m, null, true);
        }

        [Test]
        public void TestDecimal()
        {
            TestSimpleValues(new SqlDataType(SqlDbType.Decimal, 28, 0), -53m, 42m, true);
            TestSimpleValues(new SqlDataType(SqlDbType.Decimal, 28, 0), 61m, -75m, false);
            TestSimpleValues(new SqlDataType(SqlDbType.Decimal, 4, 0), -5217m, 5829m, true);
            TestSimpleValues(new SqlDataType(SqlDbType.Decimal, 4, 0), 5217m, -5829m, false);
            TestSimpleValues(new SqlDataType(SqlDbType.Decimal, 4, 2), -52.17m, 58.29m, true);
            TestSimpleValues(new SqlDataType(SqlDbType.Decimal, 4, 2), 52.17m, -58.29m, false);
            SqlDecimal one = SqlDecimal.ConvertToPrecScale(new SqlDecimal(0.5217m), 38, 38);
            SqlDecimal two = SqlDecimal.ConvertToPrecScale(new SqlDecimal(-0.5829m), 38, 38);
            TestSimpleValues(new SqlDataType(SqlDbType.Decimal, 38, 38), one, two, true);
            TestSimpleValues(new SqlDataType(SqlDbType.Decimal, 38, 38), two, one, false);
            TestSimpleValues(new SqlDataType(SqlDbType.Decimal, 38, 37), null, one, true);
            TestSimpleValues(new SqlDataType(SqlDbType.Decimal, 38, 37), two, null, true);
        }

        [Test]
        public void TestChar()
        {
            //we pad because that's what SQL server does on fixed length chars
            TestSimpleValues(new SqlDataType(SqlDbType.Char, 5), "foo  ", "bar  ", true);
            TestSimpleValues(new SqlDataType(SqlDbType.Char, 5), "bar  ", "foo  ", false);
            TestSimpleValues(new SqlDataType(SqlDbType.Char, 5), "foo  ", null, true);
            TestSimpleValues(new SqlDataType(SqlDbType.Char, 5), null, "foo  ", true);
            //let's try all the way to 7993 chars (maximum we can accept right now because of meta data
            //  and the fact we're using row log contents instead of the full log row)
            int max = 7993;
            StringBuilder one = new StringBuilder(max);
            StringBuilder two = new StringBuilder(max);
            for (int i = 0; i < max; i++)
            {
                one.Append((char)(65 + (i % 26)));
                two.Append((char)(90 - (i % 26)));
            }
            TestSimpleValues(new SqlDataType(SqlDbType.Char, max), one.ToString(), two.ToString(), true);
        }

        [Test]
        public void TestVarChar()
        {
            TestSimpleValues(new SqlDataType(SqlDbType.VarChar, 5), "foo", "bar", true);
            TestSimpleValues(new SqlDataType(SqlDbType.VarChar, 5), "bar", "foo", false);
            TestSimpleValues(new SqlDataType(SqlDbType.VarChar, 5), "foo", null, true);
            TestSimpleValues(new SqlDataType(SqlDbType.VarChar, 5), null, "foo", true);
            //let's try all the way to 7989 chars (maximum we can accept right now because of meta data
            //  and the fact we're using row log contents instead of the full log row)
            int max = 7989;
            StringBuilder one = new StringBuilder(max);
            StringBuilder two = new StringBuilder(max);
            for (int i = 0; i < max; i++)
            {
                one.Append((char)(65 + (i % 26)));
                two.Append((char)(90 - (i % 26)));
            }
            TestSimpleValues(new SqlDataType(SqlDbType.VarChar, max), one.ToString(), two.ToString(), true);
        }

        [Test]
        public void TestNChar()
        {
            //we pad because that's what SQL server does on fixed length chars
            TestSimpleValues(new SqlDataType(SqlDbType.NChar, 5), "foo  ", "bar  ", true);
            TestSimpleValues(new SqlDataType(SqlDbType.NChar, 5), "bar  ", "foo  ", false);
            TestSimpleValues(new SqlDataType(SqlDbType.NChar, 5), "foo  ", null, true);
            TestSimpleValues(new SqlDataType(SqlDbType.NChar, 5), null, "foo  ", true);
            //let's try all the way to 3997 chars (maximum we can accept right now because of meta data
            //  and the fact we're using row log contents instead of the full log row)
            int max = 3997;
            StringBuilder one = new StringBuilder(max);
            StringBuilder two = new StringBuilder(max);
            for (int i = 0; i < max; i++)
            {
                one.Append((char)(65 + (i % 26)));
                two.Append((char)(90 - (i % 26)));
            }
            TestSimpleValues(new SqlDataType(SqlDbType.NChar, max), one.ToString(), two.ToString(), true);
        }

        [Test]
        public void TestNVarChar()
        {
            TestSimpleValues(new SqlDataType(SqlDbType.NVarChar, 5), "foo", "bar", true);
            TestSimpleValues(new SqlDataType(SqlDbType.NVarChar, 5), "bar", "foo", false);
            TestSimpleValues(new SqlDataType(SqlDbType.NVarChar, 5), "foo", null, true);
            TestSimpleValues(new SqlDataType(SqlDbType.NVarChar, 5), null, "foo", true);
            //let's try all the way to 3995 chars (maximum we can accept right now because of meta data
            //  and the fact we're using row log contents instead of the full log row)
            int max = 3995;
            StringBuilder one = new StringBuilder(max);
            StringBuilder two = new StringBuilder(max);
            for (int i = 0; i < max; i++)
            {
                one.Append((char)(65 + (i % 26)));
                two.Append((char)(90 - (i % 26)));
            }
            TestSimpleValues(new SqlDataType(SqlDbType.NVarChar, max), one.ToString(), two.ToString(), true);
        }

        [Test]
        public void TestBinary()
        {
            byte[] one = new byte[] { 4, 12, 3, 3, 5, 0, 0, 0, 0, 0 };
            byte[] two = new byte[] { 33, 234, 53, 23, 31, 55, 53, 1, 4, 0 };
            TestSimpleValues(new SqlDataType(SqlDbType.Binary, 10), one, two, true);
            TestSimpleValues(new SqlDataType(SqlDbType.Binary, 10), two, one, false);
            TestSimpleValues(new SqlDataType(SqlDbType.Binary, 10), one, null, true);
            TestSimpleValues(new SqlDataType(SqlDbType.Binary, 10), null, two, true);
        }

        [Test]
        public void TestVarBinary()
        {
            //it is important to note that bytes at the end of the array that match are ignored
            byte[] one = new byte[] { 4, 12, 3, 3, 5, 0, 0, 0, 0, 0 };
            byte[] two = new byte[] { 33, 234, 53, 23, 31, 55, 53, 1, 4, 1 };
            TestSimpleValues(new SqlDataType(SqlDbType.VarBinary, 100), one, two, true);
            TestSimpleValues(new SqlDataType(SqlDbType.VarBinary, 100), two, one, false);
            //try a blank array
            //TODO: fix this...
            //TestSimpleValues(new SqlDataType(SqlDbType.VarBinary, 100), new byte[0], null, false);
            //TestSimpleValues(new SqlDataType(SqlDbType.VarBinary, 100), null, new byte[0], false);
        }

        private void TestSimpleValues(SqlDbType key, object val, object newVal, bool nullable)
        {
            TestSimpleValues(new SqlDataType(key), val, newVal, nullable);
        }

        private void TestSimpleValues(SqlDataType key, object val, object newVal, bool nullable)
        {
            TestSimpleValues(new SqlDataType[] { key }, new object[] { val },
                new object[] { newVal }, nullable);
        }

        private void TestSimpleValues(SqlDataType[] keys, object[] valObjects, object[] newValObjects, bool nullable)
        {
            Dictionary<SqlDataType, object> vals = new Dictionary<SqlDataType, object>(keys.Length);
            Dictionary<SqlDataType, object> newVals = new Dictionary<SqlDataType, object>(keys.Length);
            for (int i = 0; i < keys.Length; i++)
            {
                vals[keys[i]] = valObjects[i];
                newVals[keys[i]] = newValObjects[i];
            }
            TestSimpleValues(vals, newVals, nullable);
        }

        private void TestSimpleValues(Dictionary<SqlDataType, object> vals,
            Dictionary<SqlDataType, object> newVals, bool nullable)
        {
            //create table
            TestUtils.CreateDataTypesTable("LogReaderTest", vals.Keys, nullable);
            //do insert
            TestUtils.InsertIntoDataTypesTable("LogReaderTest", vals);
            //grab record
            RecordInfo record = TestUtils.GetLatestRecord("LogReaderTest", Operation.Insert);
            //check insert
            Assert.IsNotNull(record.NewRow);
            AssertRowValues(record.NewRow, vals);
            //do update
            TestUtils.UpdateDataTypesTable("LogReaderTest", newVals);
            //grab record
            record = TestUtils.GetLatestRecord("LogReaderTest", Operation.Update);
            Assert.IsNotNull(record.OriginalRow);
            AssertRowValues(record.OriginalRow, vals);
            Assert.IsNotNull(record.NewRow);
            AssertRowValues(record.NewRow, newVals);
            //do delete
            TestUtils.DeleteFromDataTypesTable("LogReaderTest");
            //grab record
            record = TestUtils.GetLatestRecord("LogReaderTest", Operation.Delete);
            //check delete
            Assert.IsNotNull(record.OriginalRow);
            AssertRowValues(record.OriginalRow, newVals);
            //drop table
            TestUtils.DropTableIfExists("LogReaderTest");
        }

        private void AssertRowValues(RowInfo row, Dictionary<SqlDataType, object> vals)
        {
            foreach (KeyValuePair<SqlDataType, object> val in vals)
            {
                Assert.AreEqual(val.Value, row[Enum.GetName(typeof(SqlDbType), val.Key.Type) + "Col"],
                    "Invalid row value for type " + Enum.GetName(typeof(SqlDbType), val.Key.Type));
            }
        }
    }
}
