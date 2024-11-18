using Dapper;
using Migrasi.Helpers;
using Spectre.Console;
using Spectre.Console.Cli;

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
                Utils.WriteLogMessage("Proses piutang");
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringBilling,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "rekening_air",
                    queryPath: @"Queries\piutang.sql",
                    parameters: new()
                    {
                                                { "@idpdam", settings.IdPdam }
                    });

                Utils.WriteLogMessage("Proses piutang detail");
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringBilling,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "rekening_air_detail",
                    queryPath: @"Queries\piutang_detail.sql",
                    parameters: new()
                    {
                                                { "@idpdam", settings.IdPdam }
                    });

                AnsiConsole.MarkupLine($"[bold green]Migrasi data piutang finish[/]");
            }
            catch (Exception)
            {
                throw;
            }

            return 0;
        }
    }
}
