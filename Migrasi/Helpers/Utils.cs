using MySqlConnector;
using Spectre.Console;

namespace Migrasi.Helpers
{
    public static class Utils
    {
        public static void WriteLogMessage(string message)
        {
            AnsiConsole.MarkupLine($"[grey]LOG:[/] {message}[grey]...[/]");
        }

        public static async Task BulkCopy(string sConnectionStr, string tConnectionStr, string queryPath, string tableName, Dictionary<string, object?>? parameters = null, Dictionary<string, string>? placeholders = null)
        {
            using var sConnection = new MySqlConnection(sConnectionStr);
            await sConnection.OpenAsync();

            using var tConnection = new MySqlConnection(tConnectionStr);
            await tConnection.OpenAsync();
            var trans = await tConnection.BeginTransactionAsync();

            try
            {
                string query = await File.ReadAllTextAsync(queryPath);

                if (placeholders != null)
                {
                    foreach (var item in placeholders)
                    {
                        query = query.Replace(item.Key, item.Value);
                    }
                }

                var cmd = new MySqlCommand(query, sConnection);

                if (parameters?.Count > 0)
                {
                    foreach (var p in parameters)
                    {
                        cmd.Parameters.AddWithValue(p.Key, p.Value);
                    }
                }

                using MySqlDataReader reader = await cmd.ExecuteReaderAsync();

                var bulkCopy = new MySqlBulkCopy(tConnection, trans)
                {
                    DestinationTableName = tableName,
                    ConflictOption = MySqlBulkLoaderConflictOption.Replace,
                };

                await bulkCopy.WriteToServerAsync(reader);
                await trans.CommitAsync();
            }
            catch (Exception)
            {
                await trans.RollbackAsync();
                throw;
            }
            finally
            {
                await sConnection.CloseAsync();
                await MySqlConnection.ClearPoolAsync(sConnection);

                await tConnection.CloseAsync();
                await MySqlConnection.ClearPoolAsync(tConnection);
            }
        }

        public static async Task Client(Func<MySqlConnection, MySqlTransaction?, Task> operations)
        {
            using var conn = new MySqlConnection(AppSettings.ConnectionString);
            await conn.OpenAsync();
            var trans = await conn.BeginTransactionAsync();

            try
            {
                await operations(conn, trans);
                await trans.CommitAsync();
            }
            catch (Exception)
            {
                await trans.RollbackAsync();
                throw;
            }
            finally
            {
                await conn.CloseAsync();
                await MySqlConnection.ClearPoolAsync(conn);
            }
        }

        public static async Task ClientBilling(Func<MySqlConnection, MySqlTransaction?, Task> operations)
        {
            using var conn = new MySqlConnection(AppSettings.ConnectionStringBilling);
            await conn.OpenAsync();
            var trans = await conn.BeginTransactionAsync();

            try
            {
                await operations(conn, trans);
                await trans.CommitAsync();
            }
            catch (Exception)
            {
                await trans.RollbackAsync();
                throw;
            }
            finally
            {
                await conn.CloseAsync();
                await MySqlConnection.ClearPoolAsync(conn);
            }
        }

        public static async Task ClientBacameter(Func<MySqlConnection, MySqlTransaction?, Task> operations)
        {
            using var conn = new MySqlConnection(AppSettings.ConnectionStringBacameter);
            await conn.OpenAsync();
            var trans = await conn.BeginTransactionAsync();

            try
            {
                await operations(conn, trans);
                await trans.CommitAsync();
            }
            catch (Exception)
            {
                await trans.RollbackAsync();
                throw;
            }
            finally
            {
                await conn.CloseAsync();
                await MySqlConnection.ClearPoolAsync(conn);
            }
        }
    }
}
