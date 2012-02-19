using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;

namespace SqlChop.Core
{
    public class RowInfo
    {
        private readonly TableInfo _Table;
        private readonly Dictionary<ColumnInfo, object> _ColumnValues;

        internal RowInfo(byte[] bytes, TableInfo table, Operation operation, int? rowOffset)
        {
            _Table = table;
            _ColumnValues = new Dictionary<ColumnInfo, object>(table.Columns.Count);
            switch (operation)
            {
                case Operation.InsertRows:
                case Operation.DeleteRows:
                    ParseRowContents(bytes, 0);
                    break;
                case Operation.ModifyRow:
                    ParseRowContents(bytes, rowOffset.Value);
                    break;
            }
        }

        private void ParseRowContents(byte[] bytes, int rowOffset)
        {
            //normalize the col count
            bool onlyVarCols;
            bytes = NormalizeBytes(bytes, rowOffset, out onlyVarCols);
            //first byte is status
            //second byte is skipped
            //third and fourth is the amount of bytes for fixed width cols
            //  (+4 for the 2 here and the 2 above)
            int posOffset = BitUtil.ToInt16(bytes, 2, true);
            //number of cols is two bytes at the end of this
            int colCount = 1;
            byte[] nullability = new byte[(int)Math.Ceiling(colCount / 8d)];
            int fixedLengthColumnOffset = 4;
            int varLengthColumnCount = -1;
            int varLengthColumnCountBegin = -1;
            int varLengthColumnCurrentCount = -1;
            int varLengthColumnOffset = -1;
            if (posOffset != 0 && bytes.Length > posOffset)
            {
                //colCount
                colCount = BitUtil.ToInt16(bytes, posOffset, true);
                if (colCount != _Table.Columns.Count)
                {
                    throw new Exception("Expected " + _Table.Columns.Count + " columns, but received " +
                        colCount + " columns");
                }
                //nullability is 1 bit per column
                posOffset += 2;
                if (bytes.Length > posOffset)
                {
                    Array.Copy(bytes, posOffset, nullability, 0, nullability.Length);
                    posOffset += nullability.Length;
                    if (bytes.Length > posOffset)
                    {
                        //may not have var length columns (and may show 1-byte 0 or just not be there)
                        varLengthColumnCount = bytes.Length <= posOffset + 1 ? -1 :
                            BitUtil.ToInt16(bytes, posOffset, true);
                        varLengthColumnCountBegin = posOffset + 2;
                        varLengthColumnCurrentCount = 0;
                        varLengthColumnOffset = posOffset + 2 + (varLengthColumnCount * 2);
                    }
                }
            }
            BitArray nullabilityArray = new BitArray(nullability);
            //load up the columns
            foreach (ColumnInfo col in _Table.Columns)
            {
                if (col.IsFixedWidth && !onlyVarCols && bytes.Length > fixedLengthColumnOffset)
                {
                    if (nullabilityArray[col.OrdinalPosition - 1])
                    {
                        _ColumnValues[col] = null;
                    }
                    else
                    {
                        if (!col.DataTypeLength.HasValue)
                        {
                            Console.WriteLine();
                        }
                        int length = col.DataTypeLength.Value;
                        //sometimes, the last column is trimmed to a certain size (e.g. positive bigint can be uint24)
                        if (fixedLengthColumnOffset + length >= bytes.Length)
                        {
                            length = bytes.Length - fixedLengthColumnOffset;
                        }
                        _ColumnValues[col] = col.FromBytes(bytes, fixedLengthColumnOffset, length);
                    }
                    fixedLengthColumnOffset += col.DataTypeLength.Value;
                }
                else if (!col.IsFixedWidth && nullabilityArray[col.OrdinalPosition - 1])
                {
                    _ColumnValues[col] = null;
                    varLengthColumnCurrentCount++;
                }
                else if (varLengthColumnCount > 0 && bytes.Length > varLengthColumnOffset)
                {
                    int end = BitUtil.ToInt16(bytes, varLengthColumnCountBegin +
                        (varLengthColumnCurrentCount * 2), true);
                    //sometimes, the last column is trimmed to a certain size
                    if (end >= bytes.Length)
                    {
                        end = bytes.Length;
                    }
                    _ColumnValues[col] = col.FromBytes(bytes, varLengthColumnOffset, end - varLengthColumnOffset);
                    varLengthColumnOffset = end;
                    varLengthColumnCurrentCount++;
                }
            }
        }

        private byte[] NormalizeBytes(byte[] bytes, int rowOffset, out bool onlyVarCols)
        {
            onlyVarCols = false;
            if (rowOffset == 0)
            {
                return bytes;
            }
            byte[] ret = new byte[rowOffset + bytes.Length];
            bytes.CopyTo(ret, rowOffset);
            //who cares about status
            ret[0] = 0;
            //who cares about the second byte
            ret[1] = 0;
            //this is the offset for the column count
            if (rowOffset > 2)
            {
                if (rowOffset >= 4)
                {
                    //we'll set the col count position in a sec...
                    //skip all the fixed column lengths
                    int offset = 4;
                    foreach (ColumnInfo col in _Table.Columns)
                    {
                        if (col.IsFixedWidth)
                        {
                            offset += col.DataTypeLength.Value;
                        }
                    }
                    //do we have the byte room for col count?
                    if (ret.Length > offset)
                    {
                        //now we can set the col count position
                        BitUtil.GetBytes((short)offset, true).CopyTo(ret, 2);
                    }
                    if (rowOffset > offset)
                    {
                        onlyVarCols = true;
                        //now set the actual col count
                        BitUtil.GetBytes((short)_Table.Columns.Count, true).CopyTo(ret, offset);
                        offset += 2;
                        if (rowOffset > offset)
                        {
                            //nullability is obviously 1 col of false if we're this far (byte 254)
                            ret[offset] = 254;
                            offset++;
                            if (rowOffset > offset)
                            {
                                //must be a count of only 1
                                BitUtil.GetBytes((short)1, true).CopyTo(ret, offset);
                                offset += 2;
                                if (rowOffset > offset)
                                {
                                    //must end at the end of the array
                                    BitUtil.GetBytes((short)ret.Length, true).CopyTo(ret, offset);
                                }
                            }
                        }
                    }
                }
            }
            return ret;
        }

        public TableInfo Table { get { return _Table; } }

        public bool IsColumnPresent(int index)
        {
            return _ColumnValues.ContainsKey(_Table[index]);
        }

        public bool IsColumnPresent(string name)
        {
            return _ColumnValues.ContainsKey(_Table[name]);
        }

        public bool IsColumnPresent(ColumnInfo column)
        {
            return _ColumnValues.ContainsKey(column);
        }

        public object this[int index] { get { return _ColumnValues[_Table[index]]; } }

        public object this[string name] { get { return _ColumnValues[_Table[name]]; } }

        public object this[ColumnInfo column] { get { return _ColumnValues[column]; } }
    }
}
