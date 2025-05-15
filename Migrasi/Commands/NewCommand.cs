using Dapper;
using Migrasi.Helpers;
using Spectre.Console;
using Spectre.Console.Cli;

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
        }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            settings.IdPdam ??= AnsiConsole.Ask<int>("ID:");
            settings.NamaPdam ??= AnsiConsole.Ask<string>("Nama:");

            AnsiConsole.Write(
                new Table()
                .AddColumn(new TableColumn("Setting"))
                .AddColumn(new TableColumn("Value"))
                .AddRow("Pdam", $"{settings.IdPdam} {settings.NamaPdam}")
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
                        await Utils.TrackProgress("Setting partition", async () =>
                        {
                            await Utils.Client(async (conn, trans) =>
                            {
                                var partisiTable = await conn.QueryAsync<string>("SELECT table_name FROM information_schema.PARTITIONS WHERE table_schema=@schema AND partition_method='list' GROUP BY table_name",
                                    new { schema = AppSettings.MainDatabase }, trans);
                                if (partisiTable.Any())
                                {
                                    foreach (var table in partisiTable)
                                    {
                                        var cek = await conn.QueryFirstOrDefaultAsync<int?>("SELECT 1 FROM information_schema.PARTITIONS WHERE table_schema=@schema AND partition_method='list' AND table_name=@table AND partition_name=@partisi",
                                            new
                                            {
                                                schema = AppSettings.MainDatabase,
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
                                    DELETE FROM setting_gcs WHERE idpdam = @idpdam;
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
                            });
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\master_attribute_pdam_detail.sql",
                                table: "master_attribute_pdam_detail",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                });
                        });

                        await Utils.TrackProgress("Seting gcs", async () =>
                        {
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\setting_gcs.sql",
                                table: "setting_gcs",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                });
                        });

                        await Utils.TrackProgress("Copy setting configuration", async () =>
                        {
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\setting_configuration_sections.sql",
                                table: "setting_configuration_sections");
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\setting_configuration_items.sql",
                                table: "setting_configuration_items");
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\setting_configuration.sql",
                                table: "setting_configuration",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                });
                        });

                        await Utils.TrackProgress("Copy setting mobile", async () =>
                        {
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\setting_mobile_items.sql",
                                table: "setting_mobile_items");
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\setting_mobile.sql",
                                table: "setting_mobile",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                });
                        });

                        await Utils.TrackProgress("Setting user module role access", async () =>
                        {
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\master_user_access.sql",
                                table: "master_user_access");
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\master_user_module.sql",
                                table: "master_user_module");
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\master_user_module_access.sql",
                                table: "master_user_module_access");
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\master_user_role.sql",
                                table: "master_user_role",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                });
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\master_user_role_access.sql",
                                table: "master_user_role_access",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                });

                            await Utils.Client(async (conn, trans) =>
                            {
                                var query = await File.ReadAllTextAsync(@"Queries\Patches\setup_new_pdam.sql");
                                await conn.ExecuteAsync(query,
                                    new
                                    {
                                        idpdam = settings.IdPdam,
                                        namapdam = settings.NamaPdam
                                    }, trans);
                            });
                        });

                        await Utils.TrackProgress("Setting feature", async () =>
                        {
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\app_setting_module.sql",
                                table: "app_setting_module");
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\app_setting_pdam_module.sql",
                                table: "app_setting_pdam_module",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                });
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\app_setting_main_feature.sql",
                                table: "app_setting_main_feature");
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\app_setting_feature.sql",
                                table: "app_setting_feature");
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\app_setting_pdam_feature.sql",
                                table: "app_setting_pdam_feature",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                });
                        });

                        await Utils.TrackProgress("Setting user config & dashboard", async () =>
                        {
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\dashboard_master_access.sql",
                                table: "dashboard_master_access");
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\dashboard_user.sql",
                                table: "dashboard_user");
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringStaging,
                                targetConnection: AppSettings.MainConnectionString,
                                queryPath: @"Queries\Master\dashboard_user_access.sql",
                                table: "dashboard_user_access");
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
