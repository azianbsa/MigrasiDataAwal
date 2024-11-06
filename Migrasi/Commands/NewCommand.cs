using Spectre.Console.Cli;
using Spectre.Console;
using MySqlConnector;
using System.Diagnostics;
using Dapper;
using System.Dynamic;

namespace Migrasi.Commands
{
    public class NewCommand : AsyncCommand<NewCommand.Settings>
    {
        public class Settings : CommandSettings
        {
            [CommandOption("-i|--idpdam")]
            public int? IdPdam { get; set; }

            [CommandOption("-n|--nama-pdam")]
            public string? NamaPdam { get; set; }

            [CommandOption("-c|--copy-dari-idpdam")]
            public int? IdPdamCopy { get; set; }


            [CommandOption("-e|--environment")]
            public Environment? Environment { get; set; }
        }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            settings.IdPdam ??= AnsiConsole.Ask<int>("ID PDAM :");
            settings.NamaPdam ??= AnsiConsole.Ask<string>("Nama PDAM :");
            settings.IdPdamCopy ??= AnsiConsole.Ask<int>("Copy data dari ID PDAM :");
            settings.Environment ??= AnsiConsole.Prompt(
                    new SelectionPrompt<Environment>()
                    .Title("Target environment :")
                    .AddChoices([Environment.Development, Environment.Staging, Environment.Production]));

            AnsiConsole.Write(
                new Table()
                .AddColumn(new TableColumn("Setting"))
                .AddColumn(new TableColumn("Value"))
                .AddRow("Id pdam", settings.IdPdam.ToString()!)
                .AddRow("Nama pdam", settings.NamaPdam)
                .AddRow("Copy data dari pdam", settings.IdPdamCopy.ToString()!)
                .AddRow("Environment", settings.Environment.ToString()!));

            var proceedWithSettings = AnsiConsole.Prompt(
                new TextPrompt<bool>("Proceed with the aformentioned settings?")
                .AddChoice(true)
                .AddChoice(false)
                .DefaultValue(true)
                .WithConverter(choice => choice ? "y" : "n"));

            if (!proceedWithSettings)
            {
                return 0;
            }

            try
            {
                await AnsiConsole
                    .Status()
                    .StartAsync("Processing...",
                    async _ =>
                    {
                        using var conn = new MySqlConnection(
                            new MySqlConnectionStringBuilder
                            {
                                Server = AppSettings.DBHost,
                                Port = AppSettings.DBPort,
                                UserID = AppSettings.DBUser,
                                Password = AppSettings.DBPassword,
                                Database = AppSettings.DBName,
                                AllowUserVariables = true,
                                AllowLoadLocalInfile = true,
                                AllowZeroDateTime = true,
                            }.ConnectionString);

                        await conn.OpenAsync();
                        var trans = await conn.BeginTransactionAsync();

                        try
                        {
                            var sw = Stopwatch.StartNew();

                            Program.WriteLogMessage("Setting partition");
                            var partisiTable = await conn.QueryAsync<string>("SELECT table_name FROM information_schema.PARTITIONS WHERE table_schema=@schema AND partition_method='list' GROUP BY table_name",
                                new { schema = AppSettings.DBName }, trans);
                            if (partisiTable.Any())
                            {
                                foreach (var table in partisiTable)
                                {
                                    var cek = await conn.QueryFirstOrDefaultAsync<int?>("SELECT 1 FROM information_schema.PARTITIONS WHERE table_schema=@schema AND partition_method='list' AND table_name=@table AND partition_name=@partisi",
                                        new
                                        {
                                            schema = AppSettings.DBName,
                                            table,
                                            partisi = $"pdam{settings.IdPdam}"
                                        }, transaction: trans);
                                    if (!cek.HasValue)
                                    {
                                        await conn.ExecuteAsync($"ALTER TABLE {table} ADD PARTITION (PARTITION pdam{settings.IdPdam} VALUES IN (@value) ENGINE = INNODB)",
                                            new { value = settings.IdPdam }, trans);
                                    }
                                }
                            }

                            Program.WriteLogMessage("Setup new pdam");
                            var query = await File.ReadAllTextAsync(@"Queries\Patches\setup_new_pdam.sql");
                            await conn.ExecuteAsync(query,
                                new
                                {
                                    idpdam = settings.IdPdam,
                                    namapdam = settings.NamaPdam,
                                    idpdamcopy = settings.IdPdamCopy
                                }, trans);

                            await trans.CommitAsync();

                            sw.Stop();
                            AnsiConsole.MarkupLine($"[bold green]Setup new pdam finish (elapsed {sw.Elapsed})[/]");
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
                    });
            }
            catch (Exception)
            {
                throw;
            }

            return 0;
        }
    }
}
