using System;
using System.Collections.Generic;
using System.Text;
using NUnit.ConsoleRunner;
using System.IO;
using SqlChop.Core;

namespace SqlChop.Test
{
    class Program
    {
        [STAThread]
        static void Main(string[] args)
        {
            Runner.Main(new string[] { Directory.GetCurrentDirectory() + @"\SqlChop.Test.exe", "/wait" });
            //Console.WriteLine(new LogSequenceNumber("00000034:000000ac:0017"));
            //Console.ReadLine();
        }
    }
}
