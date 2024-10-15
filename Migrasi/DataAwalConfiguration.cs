using MySqlConnector;

namespace Migrasi
{
    class DataAwalConfiguration(string? bsbsConnectionString = "", string? loketConnectionString = "", string? bacameterConnectionString = "", string? v6ConnectionString = "")
    {
        private readonly string bsbsConnectionString = bsbsConnectionString!;
        private readonly string loketConnectionString = loketConnectionString!;
        private readonly string bacameterConnectionString = bacameterConnectionString!;
        private readonly string v6ConnectionString = v6ConnectionString!;

        public MySqlConnection GetBsbsConnection()
        {
            return new MySqlConnection(bsbsConnectionString);
        }

        public MySqlConnection GetLoketConnection()
        {
            return new MySqlConnection(loketConnectionString);
        }

        public MySqlConnection GetBacameterConnection()
        {
            return new MySqlConnection(bacameterConnectionString);
        }

        public MySqlConnection GetV6Connection()
        {
            return new MySqlConnection(v6ConnectionString);
        }
    }
}
