using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlTypes;

namespace SqlChop.Core
{
    public class ColumnInfo : IComparable<ColumnInfo>
    {
        private const double THREE_AND_ONE_THIRD = 10 / 3.0d;

        private string _Name;
        private SqlDbType _DataType;
        private int _OrdinalPosition;
        private byte? _Precision;
        private int? _Scale;
        private int? _MaxCharacterLength;
        private int? _MaxByteLength;
        private bool _IsNullable;

        public string Name { get { return _Name; } set { _Name = value; } }

        public SqlDbType DataType { get { return _DataType; } set { _DataType = value; } }

        public int OrdinalPosition { get { return _OrdinalPosition; } set { _OrdinalPosition = value; } }

        public byte? Precision { get { return _Precision; } set { _Precision = value; } }

        public int? Scale { get { return _Scale; } set { _Scale = value; } }

        public int? MaxCharacterLength { get { return _MaxCharacterLength; } set { _MaxCharacterLength = value; } }

        public int? MaxByteLength { get { return _MaxByteLength; } set { _MaxByteLength = value; } }

        public bool IsNullable { get { return _IsNullable; } set { _IsNullable = value; } }

        internal bool IsFixedWidth
        {
            get
            {
                switch (_DataType)
                {
                    case SqlDbType.Image:
                    case SqlDbType.NText:
                    case SqlDbType.NVarChar:
                    case SqlDbType.Structured:
                    case SqlDbType.Text:
                    case SqlDbType.Udt:
                    case SqlDbType.VarBinary:
                    case SqlDbType.VarChar:
                    case SqlDbType.Variant:
                    case SqlDbType.Xml:
                        return false;
                    default: return true;
                }
            }
        }

        internal object EmptyValue
        {
            get
            {
                switch (_DataType)
                {
                    case SqlDbType.VarBinary: return new byte[0];
                    case SqlDbType.VarChar: return "";
                    default: throw new Exception("Unable to get empty version of type: " + _DataType);
                }
            }
        }

        internal int? DataTypeLength
        {
            get
            {
                switch (_DataType)
                {
                    case SqlDbType.BigInt: return 8;
                    case SqlDbType.Binary: return _MaxByteLength;
                    case SqlDbType.Bit: return 1;
                    case SqlDbType.Char: return _MaxCharacterLength;
                    case SqlDbType.Date: return 8;
                    case SqlDbType.DateTime: return 8;
                    case SqlDbType.DateTime2: return 12;
                    case SqlDbType.DateTimeOffset: return 12;
                    case SqlDbType.Decimal:
                        if (_Precision <= 9)
                        {
                            return 5;
                        }
                        else if (_Precision <= 19)
                        {
                            return 9;
                        }
                        else if (_Precision <= 28)
                        {
                            return 13;
                        }
                        else if (_Precision <= 38)
                        {
                            return 17;
                        }
                        else
                        {
                            throw new Exception("> 38 precision unsupported");
                        }
                    case SqlDbType.Float: return 8;
                    case SqlDbType.Int: return 4;
                    case SqlDbType.Money: return 8;
                    case SqlDbType.NChar: return _MaxCharacterLength * 2;
                    case SqlDbType.Real: return 4;
                    case SqlDbType.SmallDateTime: return 4;
                    case SqlDbType.SmallInt: return 2;
                    case SqlDbType.SmallMoney: return 4;
                    case SqlDbType.Time: return 8;
                    case SqlDbType.Timestamp: return 8;
                    case SqlDbType.TinyInt: return 1;
                    case SqlDbType.UniqueIdentifier: return 16;
                    default: return null;
                }
            }
        }

        internal object FromBytes(byte[] bytes, int offset, int length)
        {
            switch (_DataType)
            {
                case SqlDbType.BigInt:
                    switch (length)
                    {
                        case 1: return (long)bytes[offset];
                        case 2: return (long)BitUtil.ToUInt16(bytes, offset, true);
                        case 3: return (long)BitUtil.ToUInt24(bytes, offset, true);
                        case 4: return (long)BitUtil.ToUInt32(bytes, offset, true);
                        case 8: return BitUtil.ToInt64(bytes, offset, true);
                        default: throw new Exception("Unrecognized length for bigint: " + length);
                    }
                case SqlDbType.Binary:
                    byte[] binaryBytes = new byte[DataTypeLength.Value];
                    Array.Copy(bytes, offset, binaryBytes, 0, length);
                    return binaryBytes;
                case SqlDbType.VarBinary:
                    byte[] varBinaryBytes = new byte[length];
                    Array.Copy(bytes, offset, varBinaryBytes, 0, length);
                    return varBinaryBytes;
                case SqlDbType.Bit: return (bytes[offset] & 1) != 0;
                case SqlDbType.Char:
                    byte[] charBytes = new byte[DataTypeLength.Value];
                    Array.Copy(bytes, offset, charBytes, 0, length);
                    //go ahead and set each extra 32's (spaces) if length is shorted
                    for (int i = length; i < DataTypeLength.Value; i++)
                    {
                        charBytes[i] = 32;
                    }
                    return Encoding.UTF8.GetString(charBytes);
                case SqlDbType.VarChar:
                    byte[] varCharBytes = new byte[length];
                    Array.Copy(bytes, offset, varCharBytes, 0, length);
                    return Encoding.UTF8.GetString(varCharBytes);
                case SqlDbType.Date: return new DateTime().AddDays(BitUtil.ToInt32(bytes, offset + 4, true));
                case SqlDbType.DateTime:
                    if (length < 8)
                    {
                        byte[] dateBytes = new byte[8];
                        for (int i = 0; i < 8; i++)
                        {
                            if (i < length)
                            {
                                dateBytes[i] = bytes[offset + i];
                            }
                            else
                            {
                                dateBytes[i] = 0;
                            }
                        }
                        return new DateTime(1900, 1, 1).AddDays(BitUtil.ToInt32(dateBytes, 4, true)).
                            AddMilliseconds(THREE_AND_ONE_THIRD * BitUtil.ToInt32(dateBytes, 0, true));
                    }
                    else
                    {
                        return new DateTime(1900, 1, 1).AddDays(BitUtil.ToInt32(bytes, offset + 4, true)).
                            AddMilliseconds(THREE_AND_ONE_THIRD * BitUtil.ToInt32(bytes, offset, true));
                    }
                case SqlDbType.DateTime2:
                    return new DateTime().AddDays(BitUtil.ToInt32(bytes, offset + 8, true)).
                        AddMilliseconds(1000d / Math.Pow(10, Scale.Value) * BitUtil.ToInt64(bytes, offset, true));
                case SqlDbType.DateTimeOffset: return "DateTimeOffset not supported";
                case SqlDbType.Decimal:
                    //when data type length is less than the data type length, we must pad with zeroes
                    if (length < DataTypeLength.Value)
                    {
                        byte[] decimalBytes = new byte[offset + DataTypeLength.Value];
                        Array.Copy(bytes, offset, decimalBytes, offset, length);
                        bytes = decimalBytes;
                        length = DataTypeLength.Value;
                    }
                    int[] bits = new int[4];
                    bits[0] = BitUtil.ToInt32(bytes, offset + 1, true);
                    bits[1] = length >= 9 ? BitUtil.ToInt32(bytes, offset + 5, true) : 0;
                    bits[2] = length >= 13 ? BitUtil.ToInt32(bytes, offset + 9, true) : 0;
                    bits[3] = length >= 17 ? BitUtil.ToInt32(bytes, offset + 13, true) : 0;
                    //sometimes this is bigger than C#'s max precision/scale of 28 so just return SqlDecimal
                    if (_Precision > 28 || _Scale > 28)
                    {
                        return new SqlDecimal(_Precision.Value, (byte)_Scale, bytes[offset] == 1, bits);
                    }
                    else
                    {
                        return new SqlDecimal(_Precision.Value, (byte)_Scale, bytes[offset] == 1, bits).Value;
                    }
                case SqlDbType.Float:
                    switch (length)
                    {
                        case 7:
                            return BitUtil.ToDouble(new byte[] {
                                bytes[offset], bytes[offset + 1], bytes[offset + 2], bytes[offset + 3], 
                                bytes[offset + 4], bytes[offset + 5], bytes[offset + 6], 64 }, 0, true);
                        case 8: return BitUtil.ToDouble(bytes, offset, true);
                        default: throw new Exception("Unrecognized float length: " + length);
                    }
                case SqlDbType.Image: return "Image not supported";
                case SqlDbType.Int:
                    switch (length)
                    {
                        case 1: return (int)bytes[offset];
                        case 2: return (int)BitUtil.ToUInt16(bytes, offset, true);
                        case 3: return (int)BitUtil.ToUInt24(bytes, offset, true);
                        case 4: return BitUtil.ToInt32(bytes, offset, true);
                        default: throw new Exception("Unrecognized int length : " + length);
                    }
                case SqlDbType.Money:
                    if (length < 8)
                    {
                        byte[] moneyBytes = new byte[8];
                        for (int i = 0; i < 8; i++)
                        {
                            if (i < length)
                            {
                                moneyBytes[i] = bytes[offset + i];
                            }
                            else
                            {
                                moneyBytes[i] = (byte)(i == 2 ? 1 : 0);
                            }
                        }
                        return BitUtil.ToInt64(moneyBytes, 0, true) / 10000d;
                    }
                    return BitUtil.ToInt64(bytes, offset, true) / 10000d;
                case SqlDbType.NChar:
                    byte[] ncharBytes = new byte[DataTypeLength.Value];
                    Array.Copy(bytes, offset, ncharBytes, 0, length);
                    //go ahead and set each extra 32's (spaces) if length is shorted
                    for (int i = length; i < DataTypeLength.Value; i++)
                    {
                        ncharBytes[i] = (byte)(i % 2 == 1 ? 0 : 32);
                    }
                    return Encoding.Unicode.GetString(ncharBytes);
                case SqlDbType.NVarChar:
                    //if it's an odd number, we have to add a zero to the end
                    int nvarCharLength = length + (length % 2);
                    byte[] nvarCharBytes = new byte[nvarCharLength];
                    Array.Copy(bytes, offset, nvarCharBytes, 0, length);
                    return Encoding.Unicode.GetString(nvarCharBytes);
                case SqlDbType.NText: return "NText not supported";
                case SqlDbType.Real: return BitUtil.ToSingle(bytes, offset, true);
                case SqlDbType.SmallDateTime:
                    return new DateTime(1900, 1, 1).AddDays(BitUtil.ToUInt16(bytes, offset + 2, true)).
                        AddMinutes(BitUtil.ToUInt16(bytes, offset, true));
                case SqlDbType.SmallInt:
                    return length == 1 ? (short)bytes[offset] : BitUtil.ToInt16(bytes, offset, true);
                case SqlDbType.SmallMoney:
                    switch (length)
                    {
                        case 3: return BitUtil.ToInt32(new byte[] {
                            bytes[offset], bytes[offset + 1], bytes[offset + 2], 0 }, 0, true) / 10000d;
                        case 4: return BitUtil.ToInt32(bytes, offset, true) / 10000d;
                        default: throw new Exception("Unrecognized length for small money: " + length);
                    }
                case SqlDbType.Structured: return "Structured not supported";
                case SqlDbType.Text: return "Text not supported";
                case SqlDbType.Time:
                    return TimeSpan.FromMilliseconds(1000d / Math.Pow(10, Scale.Value) * BitUtil.ToInt64(bytes, offset, true));
                case SqlDbType.Timestamp: return "Timestamp not supported";
                case SqlDbType.TinyInt: return bytes[offset];
                case SqlDbType.Udt: return "UDT's not supported";
                case SqlDbType.UniqueIdentifier:
                    byte[] guid = new byte[16];
                    Array.Copy(bytes, offset, guid, 0, 16);
                    return new Guid(guid);
                case SqlDbType.Variant: return "Variant not supported";
                case SqlDbType.Xml: return "Xml not supported";
                default: throw new NotImplementedException(_DataType + " not implemented");
            }
        }

        public int CompareTo(ColumnInfo other)
        {
            return OrdinalPosition.CompareTo(other.OrdinalPosition);
        }
    }
}
