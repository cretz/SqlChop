using System;
using System.Collections.Generic;
using System.Text;

namespace SqlChop.Core
{
    public enum Operation
    {
        Unknown,
        BeginTransaction,
        CommitTransaction,
        InsertRows,
        ModifyRow,
        ModifyColumns,
        DeleteRows
    }
}
