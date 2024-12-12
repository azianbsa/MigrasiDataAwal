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

        public static string DBHostStaging { get; set; }
        public static uint DBPortStaging { get; set; }
        public static string DBUserStaging { get; set; }
        public static string DBPasswordStaging { get; set; }
        public static string DBNameStaging { get; set; }

        public static string DBHostBilling { get; set; }
        public static uint DBPortBilling { get; set; }
        public static string DBUserBilling { get; set; }
        public static string DBPasswordBilling { get; set; }
        public static string DBNameBilling { get; set; }

        public static string DBHostBacameter { get; set; }
        public static uint DBPortBacameter { get; set; }
        public static string DBUserBacameter { get; set; }
        public static string DBPasswordBacameter { get; set; }
        public static string DBNameBacameter { get; set; }

        public static string DBHostLoket { get; set; }
        public static uint DBPortLoket { get; set; }
        public static string DBUserLoket { get; set; }
        public static string DBPasswordLoket { get; set; }
        public static string DBNameLoket { get; set; }

        public static int CommandTimeout { get; set; } = 3600;

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
            DefaultCommandTimeout = (uint)CommandTimeout,
            ConnectionTimeout = (uint)CommandTimeout,
            Pooling = false,
        }.ConnectionString;

        public static string ConnectionStringStaging => new MySqlConnectionStringBuilder
        {
            Server = DBHostStaging,
            Port = DBPortStaging,
            UserID = DBUserStaging,
            Password = DBPasswordStaging,
            Database = DBNameStaging,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
            AllowZeroDateTime = true,
        }.ConnectionString;

        public static string ConnectionStringBilling => new MySqlConnectionStringBuilder
        {
            Server = DBHostBilling,
            Port = DBPortBilling,
            UserID = DBUserBilling,
            Password = DBPasswordBilling,
            Database = DBNameBilling,
            AllowUserVariables = true,
            DefaultCommandTimeout = (uint)CommandTimeout,
            ConnectionTimeout = (uint)CommandTimeout,
            Pooling = false,
        }.ConnectionString;

        public static string ConnectionStringBacameter => new MySqlConnectionStringBuilder
        {
            Server = DBHostBacameter,
            Port = DBPortBacameter,
            UserID = DBUserBacameter,
            Password = DBPasswordBacameter,
            Database = DBNameBacameter,
            AllowUserVariables = true,
        }.ConnectionString;

        public static string ConnectionStringLoket => new MySqlConnectionStringBuilder
        {
            Server = DBHostLoket,
            Port = DBPortLoket,
            UserID = DBUserLoket,
            Password = DBPasswordLoket,
            Database = DBNameLoket,
            AllowUserVariables = true,
            DefaultCommandTimeout = (uint)CommandTimeout,
            ConnectionTimeout = (uint)CommandTimeout,
            Pooling = false,
        }.ConnectionString;
    }
}
