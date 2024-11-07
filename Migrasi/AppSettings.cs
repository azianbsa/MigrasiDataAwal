using MySqlConnector;
using Environment = Migrasi.Enums.Environment;

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

        public static string BsbsHost { get; set; }
        public static uint BsbsPort { get; set; }
        public static string BsbsUser { get; set; }
        public static string BsbsPassword { get; set; }
        public static string BsbsDBName { get; set; }

        public static string ConnectionString => new MySqlConnectionStringBuilder
        {
            Server = DBHost,
            Port = DBPort,
            UserID = DBUser,
            Password = DBPassword,
            Database = DBName,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
            AllowZeroDateTime = true,
        }.ConnectionString;

        public static string BsbsConnectionString => new MySqlConnectionStringBuilder
        {
            Server = BsbsHost,
            Port = BsbsPort,
            UserID = BsbsUser,
            Password = BsbsPassword,
            Database = BsbsDBName,
            AllowUserVariables = true,
        }.ConnectionString;
    }
}
