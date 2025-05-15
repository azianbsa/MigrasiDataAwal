using MySqlConnector;
using Environment = Migrasi.Enums.Environment;

namespace Migrasi
{
    internal static class AppSettings
    {
        public static Environment Environment { get; set; }

        public static string ConfigHost { get; set; }
        public static uint ConfigPort { get; set; }
        public static string ConfigUserId { get; set; }
        public static string ConfigPassword { get; set; }
        public static string ConfigDatabase { get; set; }
        public static string ConfigConnectionString => new MySqlConnectionStringBuilder
        {
            Server = ConfigHost,
            Port = ConfigPort,
            UserID = ConfigUserId,
            Password = ConfigPassword,
            Database = ConfigDatabase,
            AllowUserVariables = true,
        }.ConnectionString;

        public static string MainHost { get; set; }
        public static uint MainPort { get; set; }
        public static string MainUserId { get; set; }
        public static string MainPassword { get; set; }
        public static string MainDatabase { get; set; }
        public static string MainConnectionString => new MySqlConnectionStringBuilder
        {
            Server = MainHost,
            Port = MainPort,
            UserID = MainUserId,
            Password = MainPassword,
            Database = MainDatabase,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
        }.ConnectionString;

        public static string HostStaging { get; set; }
        public static uint PortStaging { get; set; }
        public static string UserStaging { get; set; }
        public static string PasswordStaging { get; set; }
        public static string DatabaseStaging { get; set; }

        public static string HostBsbs { get; set; }
        public static uint PortBsbs { get; set; }
        public static string UserBsbs { get; set; }
        public static string PasswordBsbs { get; set; }
        public static string DatabaseBsbs { get; set; }

        public static string HostBacameter { get; set; }
        public static uint PortBacameter { get; set; }
        public static string UserBacameter { get; set; }
        public static string PasswordBacameter { get; set; }
        public static string DatabaseBacameter { get; set; }

        public static string HostLoket { get; set; }
        public static uint PortLoket { get; set; }
        public static string UserLoket { get; set; }
        public static string PasswordLoket { get; set; }
        public static string DatabaseLoket { get; set; }

        public static int CommandTimeout { get; set; } = 3600;


        public static string ConnectionStringStaging => new MySqlConnectionStringBuilder
        {
            Server = HostStaging,
            Port = PortStaging,
            UserID = UserStaging,
            Password = PasswordStaging,
            Database = DatabaseStaging,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
        }.ConnectionString;

        public static string ConnectionStringBsbs => new MySqlConnectionStringBuilder
        {
            Server = HostBsbs,
            Port = PortBsbs,
            UserID = UserBsbs,
            Password = PasswordBsbs,
            Database = DatabaseBsbs,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
        }.ConnectionString;

        public static string ConnectionStringBacameter => new MySqlConnectionStringBuilder
        {
            Server = HostBacameter,
            Port = PortBacameter,
            UserID = UserBacameter,
            Password = PasswordBacameter,
            Database = DatabaseBacameter,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
        }.ConnectionString;

        public static string ConnectionStringLoket => new MySqlConnectionStringBuilder
        {
            Server = HostLoket,
            Port = PortLoket,
            UserID = UserLoket,
            Password = PasswordLoket,
            Database = DatabaseLoket,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
        }.ConnectionString;
    }
}
