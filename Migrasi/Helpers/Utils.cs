using MySqlConnector;
using Spectre.Console;
using System.Diagnostics;

namespace Migrasi.Helpers
{
    public static class Utils
    {
        public static void WriteLogMessage(string message, bool skip = false)
        {
            AnsiConsole.MarkupLine($"[grey]LOG: {DateTime.Now}[/] {message}[grey]...[/]" + (skip ? "skip" : ""));
        }

        public static async Task BulkCopy(
            string sourceConnection,
            string targetConnection,
            string table,
            string queryPath,
            Dictionary<string, object?>? parameters = null)
        {
            using var sConnection = new MySqlConnection(sourceConnection);
            await sConnection.OpenAsync();

            using var tConnection = new MySqlConnection(targetConnection);
            await tConnection.OpenAsync();
            var trans = await tConnection.BeginTransactionAsync();

            try
            {
                var columnMappings = AppSettings.ColumnMappings
                    .Where(cm => cm.TableName == table)
                    .Select(cm => cm.ToMySqlBulkCopyColumnMapping())
                    .ToList();

                if (columnMappings.Count == 0)
                {
                    Console.WriteLine($"[Warning] Column mapping untuk {table} belum ada");
                }

                string query = await File.ReadAllTextAsync(queryPath);

                foreach (var item in AppSettings.Placeholders)
                {
                    query = query.Replace(item.Key, item.Value);
                }

                var cmd = new MySqlCommand(query, sConnection)
                {
                    CommandTimeout = (int)AppSettings.CommandTimeout
                };

                if (parameters?.Count > 0)
                {
                    foreach (var p in parameters)
                    {
                        cmd.Parameters.AddWithValue(p.Key, p.Value);
                    }
                }

                using MySqlDataReader reader = await cmd.ExecuteReaderAsync();

                ValidateSchema(reader, columnMappings);

                var bulkCopy = new MySqlBulkCopy(tConnection, trans)
                {
                    DestinationTableName = table,
                    ConflictOption = MySqlBulkLoaderConflictOption.Replace,
                };

                if (columnMappings.Count > 0)
                {
                    bulkCopy.ColumnMappings.AddRange(columnMappings);
                }

                await bulkCopy.WriteToServerAsync(reader);
                await trans.CommitAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine($"BulkCopy error: {table}: {e.Message}");
                await trans.RollbackAsync();
                throw;
            }
            finally
            {
                await sConnection.CloseAsync();
                await tConnection.CloseAsync();
            }
        }

        private static void ValidateSchema(MySqlDataReader reader, List<MySqlBulkCopyColumnMapping> columnMappings)
        {
            if (columnMappings.Count == 0)
            {
                return;
            }

            var expectedColumns = columnMappings
                .OrderBy(cm => cm.SourceOrdinal)
                .Select(cm => cm.DestinationColumn)
                .ToList();

            if (reader.FieldCount != expectedColumns.Count)
            {
                throw new InvalidOperationException("Jumlah kolom pada query tidak sesuai dengan jumlah kolom yang diharapkan.");
            }

            for (int i = 0; i < reader.FieldCount; i++)
            {
                var columnName = reader.GetName(i);
                if (!string.Equals(columnName, expectedColumns[i], StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException($"Kolom '{columnName}' pada posisi {i} tidak sesuai dengan kolom yang diharapkan '{expectedColumns[i]}'.");
                }
            }
        }

        public static async Task CopyToDiffrentHost(
            string sourceConnection,
            string targetConnection,
            string table,
            string query,
            Dictionary<string, object?>? parameters = null,
            List<MySqlBulkCopyColumnMapping>? columnMappings = null)
        {
            using var sourceConn = new MySqlConnection(sourceConnection);
            await sourceConn.OpenAsync();

            using var targetConn = new MySqlConnection(targetConnection);
            await targetConn.OpenAsync();
            var trans = await targetConn.BeginTransactionAsync();

            try
            {
                var cmd = new MySqlCommand(query, sourceConn)
                {
                    CommandTimeout = (int)AppSettings.CommandTimeout
                };

                if (parameters?.Count > 0)
                {
                    foreach (var parameter in parameters)
                    {
                        cmd.Parameters.AddWithValue(parameter.Key, parameter.Value);
                    }
                }

                using MySqlDataReader reader = await cmd.ExecuteReaderAsync();
                var bulkCopy = new MySqlBulkCopy(targetConn, trans)
                {
                    DestinationTableName = table,
                    ConflictOption = MySqlBulkLoaderConflictOption.Replace,
                };

                if (columnMappings != null && columnMappings.Count != 0)
                {
                    bulkCopy.ColumnMappings.AddRange(columnMappings);
                }

                await bulkCopy.WriteToServerAsync(reader);

                await trans.CommitAsync();
            }
            catch (Exception e)
            {
                await trans.RollbackAsync();
                Console.WriteLine($"Gagal copy table: {e.Message}");
                throw;
            }
            finally
            {
                await sourceConn.CloseAsync();
                await targetConn.CloseAsync();
            }
        }

        public static async Task MainConnectionWrapper(Func<MySqlConnection, MySqlTransaction?, Task> operations)
        {
            using var conn = new MySqlConnection(AppSettings.MainConnectionString);
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
            }
        }

        public static async Task ConfigConnectionWrapper(Func<MySqlConnection, MySqlTransaction?, Task> operations)
        {
            using var conn = new MySqlConnection(AppSettings.ConfigConnectionString);
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
            }
        }

        public static async Task BsbsConnectionWrapper(Func<MySqlConnection, MySqlTransaction?, Task> operations)
        {
            using var conn = new MySqlConnection(AppSettings.BsbsConnectionString);
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
            }
        }

        public static async Task LoketConnectionWrapper(Func<MySqlConnection, MySqlTransaction?, Task> operations)
        {
            using var conn = new MySqlConnection(AppSettings.LoketConnectionString);
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
            }
        }

        public static async Task BacameterConnectionWrapper(Func<MySqlConnection, MySqlTransaction?, Task> operations)
        {
            using var conn = new MySqlConnection(AppSettings.BacameterConnectionString);
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
            }
        }

        public static async Task TrackProgress(string process, Func<Task> fn)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                await fn();
                AnsiConsole.WriteLine($"{process}...OK {sw.ElapsedMilliseconds}ms");
            }
            catch (Exception e)
            {
                AnsiConsole.WriteLine($"{process}...FAILED {sw.ElapsedMilliseconds}ms");
                AnsiConsole.WriteLine($"Error: {e.Message}");
                throw;
            }
            finally
            {
                sw.Stop();
            }
        }

        public static bool ConfirmationPrompt(string message, bool defaultChoice = true)
        {
            return AnsiConsole.Prompt(
                new TextPrompt<bool>(message)
                    .AddChoice(true)
                    .AddChoice(false)
                    .DefaultValue(defaultChoice)
                    .WithConverter(choice => choice ? "y" : "n"));
        }
    }
}
