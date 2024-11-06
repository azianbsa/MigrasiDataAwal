using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migrasi
{
    internal static class AppSettings
    {
        public static Environment Environment { get; set; }
        public static string DBHost { get; set; }
        public static uint DBPort { get; set; }
        public static string DBUser { get; set; }
        public static string DBPassword { get; set; }
        public static string DBName { get; set; }
    }
}
