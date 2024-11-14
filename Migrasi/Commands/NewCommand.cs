using Spectre.Console.Cli;
using Spectre.Console;
using Dapper;
using Migrasi.Helpers;

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
        }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            settings.IdPdam ??= AnsiConsole.Ask<int>("ID PDAM :");
            settings.NamaPdam ??= AnsiConsole.Ask<string>("Nama PDAM :");
            settings.IdPdamCopy ??= AnsiConsole.Ask<int>("Copy data dari ID PDAM :");

            AnsiConsole.Write(
                new Table()
                .AddColumn(new TableColumn("Setting"))
                .AddColumn(new TableColumn("Value"))
                .AddRow("Id pdam", settings.IdPdam.ToString()!)
                .AddRow("Nama pdam", settings.NamaPdam)
                .AddRow("Copy data dari pdam", settings.IdPdamCopy.ToString()!)
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
                await AnsiConsole
                    .Status()
                    .StartAsync("Processing...",
                    async _ =>
                    {
                        await AnsiConsole.Status()
                                .StartAsync("Sedang diproses...", async ctx =>
                                {
                                    await Utils.Client(async (conn, trans) =>
                                    {
                                        await conn.ExecuteAsync(@"SET GLOBAL foreign_key_checks = 0;", transaction: trans);
                                    });

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
                                            await conn.ExecuteAsync(@"
                                                DELETE FROM master_attribute_pdam WHERE idpdam = @idpdam;
                                                DELETE FROM master_attribute_pdam_detail WHERE idpdam = @idpdam;
                                                DELETE FROM setting_configuration WHERE idpdam = @idpdam;
                                                DELETE FROM setting_mobile WHERE idpdam = @idpdam;
                                                DELETE FROM master_user_role WHERE idpdam = @idpdam;
                                                DELETE FROM master_user_role_access WHERE idpdam = @idpdam;

                                                INSERT INTO master_attribute_pdam
                                                SELECT
                                                 @idpdam,
                                                 @namapdam,
                                                 '-' AS provinsi,
                                                 '-' AS kota,
                                                 '-' AS alamatlengkap,
                                                 'basic' AS tipe,
                                                 0 AS flaghapus,
                                                 NOW() waktuupdate;", new { idpdam = settings.IdPdam, namapdam = settings.NamaPdam }, trans);
                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringStaging,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                queryPath: @"Queries\Master\master_attribute_pdam_detail.sql",
                                                tableName: "master_attribute_pdam_detail",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam },
                                                    { "@idpdamcopy", settings.IdPdamCopy },
                                                });
                                        });
                                    });

                                    await Utils.TrackProgress("Copy setting_configuration", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            queryPath: @"Queries\Master\setting_configuration_sections.sql",
                                            tableName: "setting_configuration_sections");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            queryPath: @"Queries\Master\setting_configuration_items.sql",
                                            tableName: "setting_configuration_items");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            queryPath: @"Queries\Master\setting_configuration.sql",
                                            tableName: "setting_configuration",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                                { "@idpdamcopy", settings.IdPdamCopy },
                                            });
                                    });

                                    await Utils.TrackProgress("Copy setting_mobile", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            queryPath: @"Queries\Master\setting_mobile_items.sql",
                                            tableName: "setting_mobile_items");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            queryPath: @"Queries\Master\setting_mobile.sql",
                                            tableName: "setting_mobile",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                                { "@idpdamcopy", settings.IdPdamCopy },
                                            });
                                    });

                                    await Utils.TrackProgress("Setting user module role access", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            queryPath: @"Queries\Master\master_user_access.sql",
                                            tableName: "master_user_access");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            queryPath: @"Queries\Master\master_user_module.sql",
                                            tableName: "master_user_module");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            queryPath: @"Queries\Master\master_user_module_access.sql",
                                            tableName: "master_user_module_access");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            queryPath: @"Queries\Master\master_user_role.sql",
                                            tableName: "master_user_role",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                                { "@idpdamcopy", settings.IdPdamCopy },
                                            });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            queryPath: @"Queries\Master\master_user_role_access.sql",
                                            tableName: "master_user_role_access",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                                { "@idpdamcopy", settings.IdPdamCopy },
                                            });

                                        await Utils.Client(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\setup_new_pdam.sql");
                                            await conn.ExecuteAsync(query,
                                                new
                                                {
                                                    idpdam = settings.IdPdam,
                                                    namapdam = settings.NamaPdam,
                                                    idpdamcopy = settings.IdPdamCopy
                                                }, trans);
                                        });
                                    });

                                    await Utils.Client(async (conn, trans) =>
                                    {
                                        await conn.ExecuteAsync(@"SET GLOBAL foreign_key_checks = 1;", transaction: trans);
                                    });
                                });

                        AnsiConsole.MarkupLine($"[bold green]Setup new pdam finish[/]");
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
