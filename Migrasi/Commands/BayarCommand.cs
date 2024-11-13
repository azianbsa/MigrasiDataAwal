using Spectre.Console.Cli;
using Spectre.Console;
using System.Diagnostics;
using Dapper;
using Migrasi.Helpers;

namespace Migrasi.Commands
{
    public class BayarCommand : AsyncCommand<BayarCommand.Settings>
    {
        public class Settings : CommandSettings
        {
            [CommandOption("-i|--idpdam")]
            public int? IdPdam { get; set; }

            [CommandOption("-t|--tahun")]
            public string? Tahun { get; set; }
        }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            settings.IdPdam ??= AnsiConsole.Ask<int>("ID PDAM :");
            settings.Tahun ??= AnsiConsole.Ask<string>("Tahun :");

            string? namaPdam = "";
            await Utils.Client(async (conn, trans) =>
            {
                namaPdam = await conn.QueryFirstOrDefaultAsync<string>(@"SELECT namapdam FROM master_attribute_pdam WHERE idpdam=@idpdam", new { idpdam = settings.IdPdam }, trans);
            });

            AnsiConsole.Write(
                new Table()
                .AddColumn(new TableColumn("Setting"))
                .AddColumn(new TableColumn("Value"))
                .AddRow("Id pdam", settings.IdPdam.ToString()!)
                .AddRow("Nama pdam", namaPdam)
                .AddRow("Tahun", settings.Tahun)
                .AddRow("Billing", AppSettings.DBNameBilling)
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
                var sw = Stopwatch.StartNew();

                await AnsiConsole.Status()
                    .StartAsync("Sedang diproses...", async ctx =>
                    {
                        foreach (var tahun in settings.Tahun.Split(","))
                        {
                            ctx.Status($"Cek golongan bayar{tahun}");
                            await Utils.ClientBilling(async (conn, trans) =>
                            {
                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_golongan.sql");
                                query = query.Replace("[table]", $"bayar{tahun}");
                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                            });

                            ctx.Status($"Cek diameter bayar{tahun}");
                            await Utils.ClientBilling(async (conn, trans) =>
                            {
                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_diameter.sql");
                                query = query.Replace("[table]", $"bayar{tahun}");
                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                            });

                            ctx.Status($"Cek kelurahan bayar{tahun}");
                            await Utils.ClientBilling(async (conn, trans) =>
                            {
                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kelurahan.sql");
                                query = query.Replace("[table]", $"bayar{tahun}");
                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                            });

                            ctx.Status($"Cek kolektif bayar{tahun}");
                            await Utils.ClientBilling(async (conn, trans) =>
                            {
                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kolektif.sql");
                                query = query.Replace("[table]", $"bayar{tahun}");
                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                            });

                            ctx.Status($"Cek administrasi lain bayar{tahun}");
                            await Utils.ClientBilling(async (conn, trans) =>
                            {
                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_adm_lain.sql");
                                query = query.Replace("[table]", $"bayar{tahun}");
                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                            });

                            ctx.Status($"Cek pemeliharaan lain bayar{tahun}");
                            await Utils.ClientBilling(async (conn, trans) =>
                            {
                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_pem_lain.sql");
                                query = query.Replace("[table]", $"bayar{tahun}");
                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                            });

                            ctx.Status($"Cek retribusi lain bayar{tahun}");
                            await Utils.ClientBilling(async (conn, trans) =>
                            {
                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_ret_lain.sql");
                                query = query.Replace("[table]", $"bayar{tahun}");
                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                            });

                            ctx.Status($"Proses bayar{tahun}");
                            await Utils.TrackProgress($"Proses bayar{tahun}", async () =>
                            {
                                await Utils.BulkCopy(
                                    sConnectionStr: AppSettings.ConnectionStringBilling,
                                    tConnectionStr: AppSettings.ConnectionString,
                                    tableName: "rekening_air",
                                    queryPath: @"Queries\bayar.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    },
                                    placeholders: new()
                                    {
                                        { "[table]", $"bayar{tahun}" }
                                    });
                            });

                            ctx.Status($"Proses bayar{tahun} detail");
                            await Utils.TrackProgress($"Proses bayar{tahun} detail", async () =>
                            {
                                await Utils.BulkCopy(
                                    sConnectionStr: AppSettings.ConnectionStringBilling,
                                    tConnectionStr: AppSettings.ConnectionString,
                                    tableName: "rekening_air_detail",
                                    queryPath: @"Queries\bayar_detail.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    },
                                    placeholders: new()
                                    {
                                        { "[table]", $"bayar{tahun}" }
                                    });
                            });
                        }
                    });

                sw.Stop();
                AnsiConsole.MarkupLine($"[bold green]Migrasi data bayar ({settings.Tahun}) finish (elapsed {sw.Elapsed})[/]");
            }
            catch (Exception)
            {
                throw;
            }

            return 0;
        }
    }
}
