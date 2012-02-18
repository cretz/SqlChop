using System;
using System.Collections.Generic;
using System.Text;
using System.Globalization;
using System.Data.SqlClient;

namespace SqlChop.Core
{
    public class LogSequenceNumber
    {
        public LogSequenceNumber(SqlDataReader reader) :
            this((string)reader["Current LSN"]) { }

        private readonly int _VirtualLogFileSequence;
        private readonly int _LogBlockOffset;
        private readonly int _SlotNumber;

        public LogSequenceNumber(string str)
        {
            string[] pieces = str.Split(':');
            _VirtualLogFileSequence = int.Parse(pieces[0], NumberStyles.HexNumber);
            _LogBlockOffset = int.Parse(pieces[1], NumberStyles.HexNumber);
            _SlotNumber = int.Parse(pieces[2], NumberStyles.HexNumber);
        }

        public override string ToString()
        {
            return _VirtualLogFileSequence + ":" + _LogBlockOffset +
                ":" + _SlotNumber;
        }
    }
}
