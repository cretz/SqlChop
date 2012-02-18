using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace SqlChop.Test
{
    internal class SqlDataType
    {
        private readonly SqlDbType _Type;
        private readonly int? _Length;
        private readonly int? _Precision;
        private readonly int? _Scale;

        public SqlDataType(SqlDbType type) : this(type, null, null, null) { }

        public SqlDataType(SqlDbType type, int length) : this(type, length, null, null) { }

        public SqlDataType(SqlDbType type, int precision, int scale) : this(type, null, precision, scale) { }

        private SqlDataType(SqlDbType type, int? length, int? precision, int? scale)
        {
            _Type = type;
            _Length = length;
            _Precision = precision;
            _Scale = scale;
        }

        public SqlDbType Type { get { return _Type; } }

        public int? Length { get { return _Length; } }

        public int? Precision { get { return _Precision; } }

        public int? Scale { get { return _Scale; } }
    }
}
