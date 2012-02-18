using System;
using System.Collections.Generic;
using System.Text;
using NUnit.ConsoleRunner;
using System.IO;

namespace SqlChop.Test
{
    class Program
    {
        [STAThread]
        static void Main(string[] args)
        {
            Runner.Main(new string[] { Directory.GetCurrentDirectory() + @"\SqlChop.Test.exe", "/wait" });
        }
    }
}
