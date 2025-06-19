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
            [CommandArgument(0, "<idpdam>")]
            public int? IdPdam { get; set; }

            [CommandArgument(1, "<nama>")]
            public string? NamaPdam { get; set; }

            [CommandArgument(2, "[idpdamcopy]")]
            public int? IdPdamCopy { get; set; } = 1;
        }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            AnsiConsole.WriteLine($"{settings.IdPdam} {settings.NamaPdam}");
            AnsiConsole.WriteLine($"ID pdam copy: {settings.IdPdamCopy}");

            if (!Utils.ConfirmationPrompt("Yakin untuk melanjutkan?"))
            {
                return 0;
            }

            try
            {
                await AnsiConsole.Status()
                    .StartAsync("Sedang diproses...", async _ =>
                    {
                        await Utils.TrackProgress("setup partition", async () =>
                        {
                            await SetupPartition(settings);
                        });

                        await Utils.TrackProgress("setup db config", async () =>
                        {
                            await SetupDbConfig(settings);
                        });

                        await Utils.TrackProgress("setup pdam", async () =>
                        {
                            await SetupPdam(settings);
                        });
                    });
            }
            catch (Exception)
            {
                throw;
            }

            return 0;
        }

        public static async Task SetupPartition(Settings settings)
        {
            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                var partisiTable = await conn.QueryAsync<string>(
                    sql: "SELECT table_name FROM information_schema.PARTITIONS WHERE table_schema=@schema AND partition_method='list' GROUP BY table_name",
                    param: new { schema = AppSettings.MainDatabase },
                    transaction: trans);
                if (partisiTable.Any())
                {
                    foreach (var table in partisiTable)
                    {
                        var cek = await conn.QueryFirstOrDefaultAsync<int?>(
                            sql: "SELECT 1 FROM information_schema.PARTITIONS WHERE table_schema=@schema AND partition_method='list' AND table_name=@table AND partition_name=@partisi",
                            param: new
                            {
                                schema = AppSettings.MainDatabase,
                                table = table,
                                partisi = $"pdam{settings.IdPdam}"
                            },
                            transaction: trans);
                        if (!cek.HasValue)
                        {
                            await conn.ExecuteAsync(
                                sql: $"ALTER TABLE {table} ADD PARTITION (PARTITION pdam{settings.IdPdam} VALUES IN (@value) ENGINE = INNODB)",
                                param: new { value = settings.IdPdam },
                                transaction: trans);
                        }
                    }
                }
            });
        }

        public static async Task SetupPdam(Settings settings)
        {
            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                var query = await File.ReadAllTextAsync(@"queries\\bacameter\setup_pdam.sql");
                await conn.ExecuteAsync(
                    sql: query,
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        namapdam = settings.NamaPdam,
                        idpdamcopy = settings.IdPdamCopy
                    },
                    transaction: trans);
            });
        }

        public async Task SetupDbConfig(Settings settings)
        {
            await Utils.ConfigConnectionWrapper(async (conn, trans) =>
            {
                var query = await File.ReadAllTextAsync(@"queries\\bacameter\setup_config.sql");
                await conn.ExecuteAsync(
                    sql: query,
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        namapdam = settings.NamaPdam,
                        idpdamcopy = settings.IdPdamCopy
                    },
                    transaction: trans);
            });
        }
    }
}
