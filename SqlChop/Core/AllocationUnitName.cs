using System;
using System.Collections.Generic;
using System.Text;

namespace SqlChop.Core
{
    public class AllocationUnitName
    {
        internal static AllocationUnitName FromString(string str)
        {
            string extra = null;
            string[] pieces = str.Split('.');
            if (pieces.Length < 2)
            {
                return null;
            }
            for (int i = 2; i < pieces.Length; i++)
            {
                if (extra == null)
                {
                    extra = pieces[i];
                }
                else
                {
                    extra += '.' + pieces[i];
                }
            }
            return new AllocationUnitName(pieces[0], pieces[1], extra);
        }

        private readonly string _SchemaName;
        private readonly string _ObjectName;
        private readonly string _Extra;

        internal AllocationUnitName(string schemaName, string objectName, string extra)
        {
            _SchemaName = schemaName;
            _ObjectName = objectName;
            _Extra = extra;
        }

        public string SchemaName { get { return _SchemaName; } }

        public string ObjectName { get { return _ObjectName; } }

        public string Extra { get { return _Extra; } }
    }
}
