using Spectre.Console.Cli;
using Spectre.Console;
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
                AnsiConsole.MarkupLine($"[bold green]Migrasi data bayar finish[/]");
            }
            catch (Exception)
            {
                throw;
            }

            return 0;
        }
    }
}
