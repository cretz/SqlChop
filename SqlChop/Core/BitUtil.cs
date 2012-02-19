using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.InteropServices;

namespace SqlChop.Core
{
    internal static class BitUtil
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

        private static byte[] ToBytes(long val, int count, bool littleEndian)
        {
            byte[] bytes = new byte[count];
            if (littleEndian)
            {
                for (int i = 0; i < count; i++)
                {
                    bytes[i] = unchecked((byte)(val & 0xff));
                    val = val >> 8;
                }
            }
            else
            {
                for (int i = 0; i < count; i++)
                {
                    bytes[count - i - 1] = unchecked((byte)(val & 0xff));
                    val = val >> 8;
                }
            }
            return bytes;
        }

        public static byte[] GetBytes(short val, bool littleEndian)
        {
            return ToBytes(val, 2, littleEndian);
        }

        public static short ToInt16(byte[] bytes, int offset, bool littleEndian)
        {
            return unchecked((short)(FromBytes(bytes, offset, 2, littleEndian)));
        }

        public static int ToInt32(byte[] bytes, int offset, bool littleEndian)
        {
            return unchecked((int)(FromBytes(bytes, offset, 4, littleEndian)));
        }

        public static long ToInt64(byte[] bytes, int offset, bool littleEndian)
        {
            return FromBytes(bytes, offset, 8, littleEndian);
        }

        public static ushort ToUInt16(byte[] bytes, int offset, bool littleEndian)
        {
            return unchecked((ushort)(FromBytes(bytes, offset, 2, littleEndian)));
        }

        public static uint ToUInt24(byte[] bytes, int offset, bool littleEndian)
        {
            return unchecked((uint)FromBytes(bytes, offset, 3, littleEndian));
        }

        public static uint ToUInt32(byte[] bytes, int offset, bool littleEndian)
        {
            return unchecked((uint)FromBytes(bytes, offset, 4, littleEndian));
        }

        public static float ToSingle(byte[] bytes, int offset, bool littleEndian)
        {
            return new Int32SingleUnion(ToInt32(bytes, offset, littleEndian)).AsSingle;
        }

        public static double ToDouble(byte[] bytes, int offset, bool littleEndian)
        {
            return BitConverter.Int64BitsToDouble(ToInt64(bytes, offset, littleEndian));
        }

        //thanks to Skeet's MiscUtils...
        [StructLayout(LayoutKind.Explicit)]
        private struct Int32SingleUnion
        {
            [FieldOffset(0)]
            int i;
            [FieldOffset(0)]
            float f;
            
            internal Int32SingleUnion(int i)
            {
                this.f = 0;
                this.i = i;
            }

            internal Int32SingleUnion(float f)
            {
                this.i = 0;
                this.f = f;
            }

            internal int AsInt32
            {
                get { return i; }
            }

            internal float AsSingle
            {
                get { return f; }
            }
        }
    }
}
