using Dapper;
using Migrasi.Helpers;
using Spectre.Console;
using Spectre.Console.Cli;
using System.ComponentModel;

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

            [CommandOption("-s|--sumber")]
            [Description("Copy data dari pdam existing")]
            public int? IdPdamCopy { get; set; }
        }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            settings.IdPdam ??= AnsiConsole.Ask<int>("ID:");
            settings.NamaPdam ??= AnsiConsole.Ask<string>("Nama:");
            settings.IdPdamCopy ??= AnsiConsole.Ask<int>("Sumber:");

            AnsiConsole.Write(
                new Table()
                .AddColumn(new TableColumn("Setting"))
                .AddColumn(new TableColumn("Value"))
                .AddRow("Pdam", $"{settings.IdPdam} {settings.NamaPdam}")
                .AddRow("Sumber", $"Pdam {settings.IdPdamCopy}")
                .AddRow("Environment", AppSettings.Environment.ToString()));

            if (!Utils.ConfirmationPrompt("Yakin untuk melanjutkan?"))
            {
                return 0;
            }

            try
            {
                await AnsiConsole.Status()
                    .StartAsync("Sedang diproses...", async ctx =>
                    {
                        await Utils.TrackProgress("setting partition", async () =>
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

                        await Utils.TrackProgress("setup pdam", async () =>
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

                AnsiConsole.MarkupLine("");
                AnsiConsole.MarkupLine($"[bold green]Setup {settings.NamaPdam} finish.[/]");
            }
            catch (Exception)
            {
                throw;
            }

            return 0;
        }
    }
}
