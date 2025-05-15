using Dapper;
using Microsoft.Data.Sqlite;
using MySqlConnector;
using Spectre.Console;
using SQLitePCL;
using System.Diagnostics;

namespace Migrasi.Helpers
{
    public static class Utils
    {
        public static void WriteLogMessage(string message, bool skip = false)
        {
            AnsiConsole.MarkupLine($"[grey]LOG: {DateTime.Now}[/] {message}[grey]...[/]" + (skip ? "skip" : ""));
        }

        public static void WriteErrMessage(Exception exception, string process, string message)
        {
            AnsiConsole.MarkupLine($"[red]ERR: {DateTime.Now}[/] process: {process}");
            AnsiConsole.MarkupLine($"[red]ERR: {DateTime.Now}[/] message: {message}");
            AnsiConsole.WriteException(exception, ExceptionFormats.ShortenEverything);
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

                var cmd = new MySqlCommand(query, sConnection)
                {
                    CommandTimeout = AppSettings.CommandTimeout
                };

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

                var result = await bulkCopy.WriteToServerAsync(reader);
                WriteLogMessage($"RowsInserted ({tableName}): {result.RowsInserted}");
                await trans.CommitAsync();
            }
            catch (Exception e)
            {
                WriteErrMessage(exception: e, process: tableName, message: e.InnerException?.Message ?? e.Message);
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
                await MySqlConnection.ClearPoolAsync(conn);
            }
        }

        public static async Task ClientConfig(Func<MySqlConnection, MySqlTransaction?, Task> operations)
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
                await MySqlConnection.ClearPoolAsync(conn);
            }
        }

        public static async Task ClientBsbs(Func<MySqlConnection, MySqlTransaction?, Task> operations)
        {
            using var conn = new MySqlConnection(AppSettings.ConnectionStringBsbs);
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

        public static async Task ClientLoket(Func<MySqlConnection, MySqlTransaction?, Task> operations)
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

        public static async Task TrackProgress(string process, Func<Task> fn)
        {
            using SqliteConnection conn = await SqliteConnectionFactory();
            var cek = await conn.QueryFirstOrDefaultAsync("SELECT nama,flagproses FROM proses_manager WHERE trim(nama)=@nama", new { nama = process });
            if (cek != null)
            {
                if (cek.flagproses == 1)
                {
                    WriteLogMessage(message: process, skip: true);
                    return;
                };
            }

            var sw = Stopwatch.StartNew();
            try
            {
                WriteLogMessage($"{process}");
                await fn();
                await conn.ExecuteAsync("REPLACE INTO proses_manager VALUES (@nama,@flagproses)", new { nama = process, flagproses = 1 });
            }
            catch (Exception e)
            {
                WriteErrMessage(exception: e, process: process, message: e.InnerException?.Message ?? e.Message);
                await conn.ExecuteAsync("REPLACE INTO proses_manager VALUES (@nama,@flagproses)", new { nama = process, flagproses = 0 });
                throw;
            }
            finally
            {
                sw.Stop();
                AnsiConsole.MarkupLine($"[grey]LOG: {DateTime.Now}[/] {process}[bold green] finish (elapsed {sw.Elapsed})[/]");
            }
        }

        public static async Task<SqliteConnection> SqliteConnectionFactory()
        {
            Batteries.Init();
            var conn = new SqliteConnection("Data Source=database.db");
            await conn.OpenAsync();
            return conn;
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
