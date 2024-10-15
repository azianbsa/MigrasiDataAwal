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
        readonly MySqlConnection sConnection;
        readonly MySqlConnection tConnection;

        public DataAwal(string processName, string tableName, string queryPath, DataAwalConfiguration configuration, Dictionary<string, object?>? parameter = null, int? drdTahunBulan = null)
        {
            this.ProcessName = processName;
            this.tableName = tableName;
            this.queryPath = queryPath;
            this.Parameter = parameter;
            this.DrdTahunBulan = drdTahunBulan;
            this.sConnection = configuration.GetSourceConnection();
            this.tConnection = configuration.GetTargetConnection();
        }

        public async Task ProsesAsync()
        {
            var sw = Stopwatch.StartNew();
            Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss zzz}] {ProcessName}...Starting");

            await sConnection.OpenAsync();
            await tConnection.OpenAsync();
            var trans = await tConnection.BeginTransactionAsync();
            MySqlBulkCopyResult? result;

            try
            {
                var cmd = await GetCommandWithParameter();
                using MySqlDataReader reader = await cmd.ExecuteReaderAsync();
                var bulkCopy = new MySqlBulkCopy(tConnection, trans)
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
                await sConnection.CloseAsync();
                await tConnection.CloseAsync();
                await MySqlConnection.ClearPoolAsync(sConnection);
                await MySqlConnection.ClearPoolAsync(tConnection);
            }

            sw.Stop();
            Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss zzz}] {ProcessName}...Finish ({result?.RowsInserted.ToString("N0", CultureInfo.CreateSpecificCulture("id-ID"))} rows) ({sw.Elapsed})");
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
            var cmd = new MySqlCommand(await GetQuery(), sConnection);

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
                Parameters  : {string.Join(", ",param)}

                """;
        }
    }
}
