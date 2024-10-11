using MySqlConnector;
using System.Diagnostics;
using System.Globalization;

namespace Migrasi
{
    class DataAwal
    {
        readonly string processName;
        readonly string tableName;
        readonly string queryPath;
        readonly int? drdTahunBulan;
        readonly Dictionary<string, object>? parameter;
        readonly MySqlConnection sConnection;
        readonly MySqlConnection tConnection;

        public DataAwal(string processName, string tableName, string queryPath, Dictionary<string, object>? parameter, DataAwalConfiguration configuration, int? drdTahunBulan = null)
        {
            this.processName = processName;
            this.tableName = tableName;
            this.queryPath = queryPath;
            this.parameter = parameter;
            this.drdTahunBulan = drdTahunBulan;
            this.sConnection = configuration.GetSourceConnection();
            this.tConnection = configuration.GetTargetConnection();
        }

        public async Task ProsesAsync()
        {
            var sw = Stopwatch.StartNew();
            Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss zzz}] {processName}...Starting");

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
                Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss zzz}] error ({processName}): {e.Message}");
                Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss zzz}] rolling back ({processName})");
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
            Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss zzz}] {processName}...Finish ({result?.RowsInserted.ToString("N0", CultureInfo.CreateSpecificCulture("id-ID"))} rows) ({sw.Elapsed})");
        }

        private async Task<string> GetQuery()
        {
            string query = await File.ReadAllTextAsync(queryPath);
            if (drdTahunBulan != null)
            {
                query = query.Replace("[tahunbulan]", drdTahunBulan.ToString());
            }
            return query;
        }

        private async Task<MySqlCommand> GetCommandWithParameter()
        {
            var cmd = new MySqlCommand(await GetQuery(), sConnection);

            if (parameter?.Count > 0)
            {
                foreach (var p in parameter)
                {
                    cmd.Parameters.AddWithValue(p.Key, p.Value);
                }
            }

            return cmd;
        }
    }
}
