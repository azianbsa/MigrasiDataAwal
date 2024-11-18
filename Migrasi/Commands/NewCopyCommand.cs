﻿using Dapper;
using Migrasi.Helpers;
using Spectre.Console;
using Spectre.Console.Cli;

namespace Migrasi.Commands
{
    public class NewCopyCommand : AsyncCommand<NewCopyCommand.Settings>
    {
        public class Settings : CommandSettings
        {
            [CommandOption("-i|--idpdam")]
            public int? IdPdam { get; set; }

            [CommandOption("-n|--nama-pdam")]
            public string? NamaPdam { get; set; }

            [CommandOption("-c|--copy-dari-pdam")]
            public int? IdPdamCopy { get; set; }
        }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            settings.IdPdam ??= AnsiConsole.Ask<int>("ID PDAM :");
            settings.NamaPdam ??= AnsiConsole.Ask<string>("Nama PDAM :");

            AnsiConsole.Write(
                new Table()
                .AddColumn(new TableColumn("Setting"))
                .AddColumn(new TableColumn("Value"))
                .AddRow("Id pdam", settings.IdPdam.ToString()!)
                .AddRow("Nama pdam", settings.NamaPdam)
                .AddRow("Environment", AppSettings.Environment.ToString()));

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
                await AnsiConsole.Status()
                    .StartAsync("Sedang diproses...", async ctx =>
                    {
                        await Utils.TrackProgress("Setting partition", async () =>
                        {
                            await Utils.Client(async (conn, trans) =>
                            {
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
                            });
                        });

                        await Utils.TrackProgress("Setup pdam", async () =>
                        {
                            await Utils.Client(async (conn, trans) =>
                            {
                                var query = await File.ReadAllTextAsync(@"Queries\Patches\setup_new_copy_pdam.sql");
                                await conn.ExecuteAsync(query,
                                    new
                                    {
                                        idpdam = settings.IdPdam,
                                        namapdam = settings.NamaPdam,
                                        idpdamcopy = settings.IdPdamCopy
                                    }, trans);
                            });
                        });
                    });

                AnsiConsole.MarkupLine($"[bold green]Setup pdam {settings.NamaPdam} finish[/]");
            }
            catch (Exception)
            {
                throw;
            }

            return 0;
        }
    }
}
