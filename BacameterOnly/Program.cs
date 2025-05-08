using Spectre.Console;
using Spectre.Console.Cli;
using System.ComponentModel;
using System.Reflection;

namespace BacameterOnly
{
    internal class Program
    {
        static int Main(string[] args)
        {
            var app = new CommandApp();

            app.Configure(config =>
            {
#if DEBUG
                config.PropagateExceptions();
#endif
                config.AddCommand<RegisterCommand>("register");
                config.AddCommand<MigrateCommand>("migrate");
                config.AddCommand<VersionCommand>("version")
                    .IsHidden();
            });

            return app.Run(args);
        }
    }

    class RegisterCommand : AsyncCommand<RegisterCommand.Settings>
    {
        public class Settings : CommandSettings
        {
            [CommandOption("--name")]
            public string? Name { get; set; }
        }
        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            var id = 1;
            settings.Name ??= AnsiConsole.Prompt(
                new TextPrompt<string>("Nama PDAM?"));
            
            var pdam = new List<dynamic>() {
                new { Id = 1, Name = "PDAM 1" },
                new { Id = 2, Name = "PDAM 2" },
                new { Id = 3, Name = "PDAM 3" }
            };

            var copyPdam = AnsiConsole.Prompt(
                new SelectionPrompt<dynamic>()
                    .Title("Pilih PDAM")
                    .AddChoices(pdam)
                    .UseConverter(x => x.GetType().GetProperty("Name")?.GetValue(x, null)?.ToString() ?? string.Empty)
                );

            var summaryTable = new Table();
            summaryTable.AddColumn(new TableColumn(""));
            summaryTable.AddColumn(new TableColumn(""));
            summaryTable.AddRow("ID", $"{id}");
            summaryTable.AddRow("Nama PDAM", $"{settings.Name}");
            summaryTable.AddRow("Copy configuration data from PDAM", $"({copyPdam.Id}) {copyPdam.Name}");
            AnsiConsole.Write(summaryTable);

            var confirmation = AnsiConsole.Prompt(
                new TextPrompt<bool>("Do you want to continue?")
                    .AddChoice(true)
                    .AddChoice(false)
                    .DefaultValue(false)
                    .WithConverter(choice => choice ? "y" : "n")
                );

            if(!confirmation)
            {
                return 0;
            }

            return await Task.FromResult(0);
        }
    }

    class MigrateCommand : Command<MigrateCommand.Settings>
    {
        public class Settings : CommandSettings
        {
        }
        public override int Execute(CommandContext context, Settings settings)
        {
            // Implement the logic for the migrate command here
            return 0;
        }
    }

    class VersionCommand : Command<VersionCommand.Settings>
    {
        public class Settings : CommandSettings { }
        public override int Execute(CommandContext context, Settings settings)
        {
            var assembly = Assembly.GetExecutingAssembly();
            Console.WriteLine($"{assembly.GetName().Name} version {assembly.GetName().Version}");
            return 0;
        }
    }
}
