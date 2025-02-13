using MySqlConnector;
using Environment = Migrasi.Enums.Environment;

namespace Migrasi
{
    internal static class AppSettings
    {
        public static Environment Environment { get; set; }
        public static string Host { get; set; }
        public static uint Port { get; set; }
        public static string User { get; set; }
        public static string Password { get; set; }
        public static string Database { get; set; }

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

        public static string ConnectionString => new MySqlConnectionStringBuilder
        {
            Server = Host,
            Port = Port,
            UserID = User,
            Password = Password,
            Database = Database,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
            AllowZeroDateTime = true,
            DefaultCommandTimeout = (uint)CommandTimeout,
            ConnectionTimeout = (uint)CommandTimeout,
            Pooling = false,
        }.ConnectionString;

        public static string ConnectionStringStaging => new MySqlConnectionStringBuilder
        {
            Server = HostStaging,
            Port = PortStaging,
            UserID = UserStaging,
            Password = PasswordStaging,
            Database = DatabaseStaging,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
            AllowZeroDateTime = true,
        }.ConnectionString;

        public static string ConnectionStringBsbs => new MySqlConnectionStringBuilder
        {
            Server = HostBsbs,
            Port = PortBsbs,
            UserID = UserBsbs,
            Password = PasswordBsbs,
            Database = DatabaseBsbs,
            AllowUserVariables = true,
            DefaultCommandTimeout = (uint)CommandTimeout,
            ConnectionTimeout = (uint)CommandTimeout,
            Pooling = false,
        }.ConnectionString;

        public static string ConnectionStringBacameter => new MySqlConnectionStringBuilder
        {
            Server = HostBacameter,
            Port = PortBacameter,
            UserID = UserBacameter,
            Password = PasswordBacameter,
            Database = DatabaseBacameter,
            AllowUserVariables = true,
        }.ConnectionString;

        public static string ConnectionStringLoket => new MySqlConnectionStringBuilder
        {
            Server = HostLoket,
            Port = PortLoket,
            UserID = UserLoket,
            Password = PasswordLoket,
            Database = DatabaseLoket,
            AllowUserVariables = true,
            DefaultCommandTimeout = (uint)CommandTimeout,
            ConnectionTimeout = (uint)CommandTimeout,
            Pooling = false,
        }.ConnectionString;
    }
}
