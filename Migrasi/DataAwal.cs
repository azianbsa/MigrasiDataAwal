using MySqlConnector;
using System.Diagnostics;
using System.Globalization;

namespace Migrasi
{
    class DataAwal
    {
        public string Key => ProcessName.Replace(" ", "_").ToLower().ToString();
        public string ProcessName;
        readonly string tableName;
        readonly string queryPath;
        public int? DrdTahunBulan;
        public Dictionary<string, object?>? Parameter;
        public SourceConnection? sourceConnection;
        readonly MySqlConnection bsbsConnection;
        readonly MySqlConnection loketConnection;
        readonly MySqlConnection bacameterConnection;
        readonly MySqlConnection v6Connection;

        public DataAwal(string processName, string tableName, string queryPath, SourceConnection? sourceConnection, DataAwalConfiguration configuration, Dictionary<string, object?>? parameter = null, int? drdTahunBulan = null)
        {
            this.ProcessName = processName;
            this.tableName = tableName;
            this.queryPath = queryPath;
            this.Parameter = parameter;
            this.DrdTahunBulan = drdTahunBulan;
            this.sourceConnection = sourceConnection;
            this.bsbsConnection = configuration.GetBsbsConnection();
            this.loketConnection = configuration.GetLoketConnection();
            this.bacameterConnection = configuration.GetBacameterConnection();
            this.v6Connection = configuration.GetV6Connection();
        }

        public async Task ProsesAsync()
        {
            var sw = Stopwatch.StartNew();
            Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss zzz}] {ProcessName}...Starting");

            GetSourceConnection(out MySqlConnection? sConnection);
            if (sConnection != null) await sConnection.OpenAsync();
            await v6Connection.OpenAsync();
            var trans = await v6Connection.BeginTransactionAsync();
            MySqlBulkCopyResult? result;

            try
            {
                var cmd = await GetCommandWithParameter();
                using MySqlDataReader reader = await cmd.ExecuteReaderAsync();
                var bulkCopy = new MySqlBulkCopy(v6Connection, trans)
                {
                    DestinationTableName = tableName,
                    ConflictOption = MySqlBulkLoaderConflictOption.Replace,
                };
                result = await bulkCopy.WriteToServerAsync(reader);
                await trans.CommitAsync();
            }
            catch (Exception e)
            {
                await trans.RollbackAsync();
                Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss zzz}] error ({ProcessName}): {e.Message}");
                Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss zzz}] rolling back ({ProcessName})");
                throw;
            }
            finally
            {
                if (sConnection != null)
                {
                    await sConnection.CloseAsync();
                    await MySqlConnection.ClearPoolAsync(sConnection);
                }

                await v6Connection.CloseAsync();
                await MySqlConnection.ClearPoolAsync(v6Connection);
            }

            sw.Stop();
            Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss zzz}] {ProcessName}...Finish ({result?.RowsInserted.ToString("N0", CultureInfo.CreateSpecificCulture("id-ID"))} rows) ({sw.Elapsed})");
        }

        private void GetSourceConnection(out MySqlConnection? connection)
        {
            connection = sourceConnection switch
            {
                SourceConnection.Bsbs => bsbsConnection,
                SourceConnection.Loket => loketConnection,
                SourceConnection.Bacameter => bacameterConnection,
                SourceConnection.V6 => v6Connection,
                _ => null
            };
        }

        private async Task<string> GetQuery()
        {
            string query = await File.ReadAllTextAsync(queryPath);
            if (DrdTahunBulan != null)
            {
                query = query.Replace("[tahunbulan]", DrdTahunBulan.ToString());
            }
            return query;
        }

        private async Task<MySqlCommand> GetCommandWithParameter()
        {
            GetSourceConnection(out MySqlConnection? connection);
            var cmd = new MySqlCommand(await GetQuery(), connection ?? v6Connection);

            if (Parameter?.Count > 0)
            {
                foreach (var p in Parameter)
                {
                    cmd.Parameters.AddWithValue(p.Key, p.Value);
                }
            }

            return cmd;
        }

        public override string ToString()
        {
            var param = new List<string>();
            if (Parameter?.Count > 0)
            {
                foreach (var p in Parameter)
                {
                    param.Add(p.Key);
                }
            }

            return $"""
                Key         : {Key}
                Name        : {ProcessName}
                Table       : {tableName}
                Query       : {queryPath}
                Parameters  : {string.Join(", ", param)}

                """;
        }
    }

    public enum SourceConnection
    {
        Bsbs,
        Loket,
        Bacameter,
        V6
    }
}
