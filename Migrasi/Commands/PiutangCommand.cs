using Spectre.Console.Cli;
using Spectre.Console;
using System.Diagnostics;
using Dapper;
using Migrasi.Helpers;

namespace Migrasi.Commands
{
    public class PiutangCommand : AsyncCommand<PiutangCommand.Settings>
    {
        public class Settings : CommandSettings
        {
            [CommandOption("-i|--idpdam")]
            public int? IdPdam { get; set; }
        }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            settings.IdPdam ??= AnsiConsole.Ask<int>("ID PDAM :");

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
                        ctx.Status("Cek bsbs golongan");
                        await Utils.ClientBilling(async (conn, trans) =>
                        {
                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_golongan.sql");
                            query = query.Replace("[table]", $"piutang");
                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                        });

                        ctx.Status("Cek bsbs diameter");
                        await Utils.ClientBilling(async (conn, trans) =>
                        {
                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_diameter.sql");
                            query = query.Replace("[table]", $"piutang");
                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                        });

                        ctx.Status("Cek bsbs kelurahan");
                        await Utils.ClientBilling(async (conn, trans) =>
                        {
                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kelurahan.sql");
                            query = query.Replace("[table]", $"piutang");
                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                        });

                        ctx.Status("Cek bsbs kolektif");
                        await Utils.ClientBilling(async (conn, trans) =>
                        {
                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kolektif.sql");
                            query = query.Replace("[table]", $"piutang");
                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                        });

                        ctx.Status("Cek bsbs administrasi lain");
                        await Utils.ClientBilling(async (conn, trans) =>
                        {
                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_adm_lain.sql");
                            query = query.Replace("[table]", $"piutang");
                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                        });

                        ctx.Status("Cek bsbs pemeliharaan lain");
                        await Utils.ClientBilling(async (conn, trans) =>
                        {
                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_pem_lain.sql");
                            query = query.Replace("[table]", $"piutang");
                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                        });

                        ctx.Status("Cek bsbs retribusi lain");
                        await Utils.ClientBilling(async (conn, trans) =>
                        {
                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_ret_lain.sql");
                            query = query.Replace("[table]", $"piutang");
                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                        });

                        ctx.Status("Proses piutang");
                        await Utils.BulkCopy(
                            sConnectionStr: AppSettings.ConnectionStringBilling,
                            tConnectionStr: AppSettings.ConnectionString,
                            tableName: "rekening_air",
                            queryPath: @"Queries\piutang.sql",
                            parameters: new()
                            {
                                { "@idpdam", settings.IdPdam }
                            });

                        ctx.Status("Proses piutang detail");
                        await Utils.BulkCopy(
                            sConnectionStr: AppSettings.ConnectionStringBilling,
                            tConnectionStr: AppSettings.ConnectionString,
                            tableName: "rekening_air_detail",
                            queryPath: @"Queries\piutang_detail.sql",
                            parameters: new()
                            {
                                { "@idpdam", settings.IdPdam }
                            });
                    });

                sw.Stop();
                AnsiConsole.MarkupLine($"[bold green]Migrasi data piutang finish (elapsed {sw.Elapsed})[/]");
            }
            catch (Exception)
            {
                throw;
            }

            return 0;
        }
    }
}
