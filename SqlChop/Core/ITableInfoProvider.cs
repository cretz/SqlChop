using System;
using System.Collections.Generic;
using System.Text;

namespace SqlChop.Core
{
    public interface ITableInfoProvider
    {
        TableInfo GetTable(string name, bool system);

        void InvalidateAllTables();

        void InvalidateTable(string name);
    }
}
