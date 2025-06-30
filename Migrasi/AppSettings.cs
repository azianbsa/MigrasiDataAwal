using MySqlConnector;
using Environment = Migrasi.Enums.Environment;

namespace Migrasi
{
    internal static class AppSettings
    {
        public static Environment Environment { get; set; }

        public static uint ConnectionTimeout { get; set; } = (uint)TimeSpan.FromMinutes(10).TotalSeconds;
        public static uint CommandTimeout { get; set; } = (uint)TimeSpan.FromMinutes(10).TotalSeconds;

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
            ConnectionTimeout = ConnectionTimeout,
            DefaultCommandTimeout = CommandTimeout,
        }.ConnectionString;

        public static string StagingHost { get; set; }
        public static uint StagingPort { get; set; }
        public static string StagingUserId { get; set; }
        public static string StagingPassword { get; set; }
        public static string StagingDatabase { get; set; }
        public static string StagingConnectionString => new MySqlConnectionStringBuilder
        {
            Server = StagingHost,
            Port = StagingPort,
            UserID = StagingUserId,
            Password = StagingPassword,
            Database = StagingDatabase,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
        }.ConnectionString;

        public static string BsbsHost { get; set; }
        public static uint BsbsPort { get; set; }
        public static string BsbsUserId { get; set; }
        public static string BsbsPassword { get; set; }
        public static string BsbsDatabase { get; set; }
        public static string BsbsConnectionString => new MySqlConnectionStringBuilder
        {
            Server = BsbsHost,
            Port = BsbsPort,
            UserID = BsbsUserId,
            Password = BsbsPassword,
            Database = BsbsDatabase,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
            ConnectionTimeout = ConnectionTimeout,
            DefaultCommandTimeout = CommandTimeout,
        }.ConnectionString;

        public static string BacameterHost { get; set; }
        public static uint BacameterPort { get; set; }
        public static string BacameterUserId { get; set; }
        public static string BacameterPassword { get; set; }
        public static string BacameterDatabase { get; set; }
        public static string BacameterConnectionString => new MySqlConnectionStringBuilder
        {
            Server = BacameterHost,
            Port = BacameterPort,
            UserID = BacameterUserId,
            Password = BacameterPassword,
            Database = BacameterDatabase,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
            ConnectionTimeout = ConnectionTimeout,
            DefaultCommandTimeout = CommandTimeout,
        }.ConnectionString;

        public static string LoketHost { get; set; }
        public static uint LoketPort { get; set; }
        public static string LoketUserId { get; set; }
        public static string LoketPassword { get; set; }
        public static string LoketDatabase { get; set; }
        public static string LoketConnectionString => new MySqlConnectionStringBuilder
        {
            Server = LoketHost,
            Port = LoketPort,
            UserID = LoketUserId,
            Password = LoketPassword,
            Database = LoketDatabase,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
            ConnectionTimeout = ConnectionTimeout,
            DefaultCommandTimeout = CommandTimeout,
        }.ConnectionString;

        public static string TampungHost { get; set; }
        public static uint TampungPort { get; set; }
        public static string TampungUserId { get; set; }
        public static string TampungPassword { get; set; }
        public static string TampungDatabase { get; set; }
        public static string TampungConnectionString => new MySqlConnectionStringBuilder
        {
            Server = TampungHost,
            Port = TampungPort,
            UserID = TampungUserId,
            Password = TampungPassword,
            Database = TampungDatabase,
            AllowUserVariables = true,
            AllowLoadLocalInfile = true,
            ConnectionTimeout = ConnectionTimeout,
            DefaultCommandTimeout = CommandTimeout,
        }.ConnectionString;

        public static List<ColumnMapping> ColumnMappings { get; set; }
        public static Dictionary<string, string> Placeholders => new()
        {
            { "[bacameter]", BacameterDatabase },
            { "[loket]", LoketDatabase },
            { "[bsbs]", BsbsDatabase },
            { "[tampung]", TampungDatabase },
            { "[dataawal]", TampungDatabase },
        };
    }
}
