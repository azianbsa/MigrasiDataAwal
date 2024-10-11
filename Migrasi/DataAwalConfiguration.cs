using MySqlConnector;

namespace Migrasi
{
    class DataAwalConfiguration(string sourceConnectionString, string targetConnectionString)
    {
        private readonly string sourceConnectionString = sourceConnectionString;
        private readonly string targetConnectionString = targetConnectionString;

        public MySqlConnection GetSourceConnection()
        {
            return new MySqlConnection(sourceConnectionString);
        }

        public MySqlConnection GetTargetConnection()
        {
            return new MySqlConnection(targetConnectionString);
        }
    }
}
