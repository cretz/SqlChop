using System;
using System.Collections.Generic;
using System.Text;

namespace SqlChop.Core
{
    internal class BitUtil
    {
        private static long FromBytes(byte[] bytes, int offset, int count, bool littleEndian)
        {
            long ret = 0;
            if (littleEndian)
            {
                for (int i = 0; i < count; i++)
                {
                    ret = unchecked((ret << 8) | bytes[offset + count - 1 - i]);
                }
            }
            else
            {
                for (int i = 0; i < count; i++)
                {
                    ret = unchecked((ret << 8) | bytes[offset + i]);
                }
            }
            return ret;
        }

        public static uint ToUInt24(byte[] bytes, int offset, bool littleEndian)
        {
            return unchecked((uint)FromBytes(bytes, offset, 3, littleEndian));
        }
    }
}
