using Dapper;
using Migrasi.Helpers;
using Spectre.Console;
using Spectre.Console.Cli;
using Sprache;

namespace Migrasi.Commands
{
    public class BacameterCommand : AsyncCommand<BacameterCommand.Settings>
    {
        public class Settings : CommandSettings
        {
            [CommandArgument(0, "<idpdam>")]
            public int IdPdam { get; set; }
        }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            const string MASTER_DATA = "Master data";
            const string PELANGGAN_AIR = "Pelanggan air";
            const string REKENING_AIR = "Rekening air";

            List<string> prosesList =
            [
                MASTER_DATA,
                PELANGGAN_AIR,
                REKENING_AIR,
            ];

            string? namaPdam = "";
            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                namaPdam = await conn.QueryFirstOrDefaultAsync<string>(
                    sql: @"SELECT namapdam FROM master_attribute_pdam WHERE idpdam=@idpdam",
                    param: new
                    {
                        idpdam = settings.IdPdam
                    },
                    transaction: trans);
            });
            Console.WriteLine($"{settings.IdPdam} {namaPdam}");

            var selectedProses = AnsiConsole.Prompt(
                new MultiSelectionPrompt<string>()
                .Title("Pilih proses:")
                .NotRequired()
                .InstructionsText("[grey](Press [blue]<space>[/] to toggle, [green]<enter>[/] to accept)[/]")
                .AddChoices(prosesList));

            var prosesMasterData = selectedProses.Exists(s => s == MASTER_DATA);
            var prosesPelangganAir = selectedProses.Exists(s => s == PELANGGAN_AIR);
            var prosesRekeningAir = selectedProses.Exists(s => s == REKENING_AIR);

            AnsiConsole.WriteLine("Proses dipilih:");
            AnsiConsole.Write(new Rows(selectedProses.Select(s => new Text($"- {s}")).ToList()));

            if (!Utils.ConfirmationPrompt(message: "Yakin untuk melanjutkan?", defaultChoice: false))
            {
                return 0;
            }

            try
            {
                await AnsiConsole.Status()
                    .StartAsync("Sedang diproses...", async ctx =>
                    {
                        if (prosesMasterData)
                        {
                            await Utils.TrackProgress("master_attribute_flag", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    queryPath: @"queries\bacameter\master_attribute_flag.sql",
                                    table: "master_attribute_flag",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_status", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_status",
                                    queryPath: @"queries\bacameter\master_attribute_status.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_jenis_bangunan", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_jenis_bangunan",
                                    queryPath: @"queries\bacameter\master_attribute_jenis_bangunan.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_kepemilikan", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_kepemilikan",
                                    queryPath: @"queries\bacameter\master_attribute_kepemilikan.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_pekerjaan", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_pekerjaan",
                                    queryPath: @"queries\bacameter\master_attribute_pekerjaan.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_peruntukan", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_peruntukan",
                                    queryPath: @"queries\bacameter\master_attribute_peruntukan.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_jenis_pipa", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_jenis_pipa",
                                    queryPath: @"queries\bacameter\master_attribute_jenis_pipa.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_kwh", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_kwh",
                                    queryPath: @"queries\bacameter\master_attribute_kwh.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_tarif_golongan", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_tarif_golongan",
                                    queryPath: @"queries\bacameter\master_tarif_golongan.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_tarif_golongan_detail", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_tarif_golongan_detail",
                                    queryPath: @"queries\bacameter\master_tarif_golongan_detail.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_tarif_diameter", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_tarif_diameter",
                                    queryPath: @"queries\bacameter\master_tarif_diameter.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_tarif_diameter_detail", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_tarif_diameter_detail",
                                    queryPath: @"queries\bacameter\master_tarif_diameter_detail.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_wilayah", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_wilayah",
                                    queryPath: @"queries\bacameter\master_attribute_wilayah.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_area", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_area",
                                    queryPath: @"queries\bacameter\master_attribute_area.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_rayon", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_rayon",
                                    queryPath: @"queries\bacameter\master_attribute_rayon.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_blok", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_blok",
                                    queryPath: @"queries\bacameter\master_attribute_blok.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_cabang", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_cabang",
                                    queryPath: @"queries\bacameter\master_attribute_cabang.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_kecamatan", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_kecamatan",
                                    queryPath: @"queries\bacameter\master_attribute_kecamatan.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_kelurahan", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_kelurahan",
                                    queryPath: @"queries\bacameter\master_attribute_kelurahan.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_dma", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_dma",
                                    queryPath: @"queries\bacameter\master_attribute_dma.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_dmz", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_dmz",
                                    queryPath: @"queries\bacameter\master_attribute_dmz.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_tarif_administrasi_lain", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_tarif_administrasi_lain",
                                    queryPath: @"queries\bacameter\master_tarif_administrasi_lain.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_tarif_pemeliharaan_lain", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_tarif_pemeliharaan_lain",
                                    queryPath: @"queries\bacameter\master_tarif_pemeliharaan_lain.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_tarif_retribusi_lain", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_tarif_retribusi_lain",
                                    queryPath: @"queries\bacameter\master_tarif_retribusi_lain.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_kolektif", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_kolektif",
                                    queryPath: @"queries\bacameter\master_attribute_kolektif.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_sumber_air", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_sumber_air",
                                    queryPath: @"queries\bacameter\master_attribute_sumber_air.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_merek_meter", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_merek_meter",
                                    queryPath: @"queries\bacameter\master_attribute_merek_meter.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_kondisi_meter", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_kondisi_meter",
                                    queryPath: @"queries\bacameter\master_attribute_kondisi_meter.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_kelainan", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BacameterConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_kelainan",
                                    queryPath: @"queries\bacameter\master_attribute_kelainan.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_petugas_baca", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BacameterConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_petugas_baca",
                                    queryPath: @"queries\bacameter\master_attribute_petugas_baca.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_periode", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_periode",
                                    queryPath: @"queries\bacameter\master_periode.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_periode_billing", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_periode_billing",
                                    queryPath: @"queries\bacameter\master_periode_billing.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_jadwal_baca", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_jadwal_baca",
                                    queryPath: @"queries\bacameter\master_attribute_jadwal_baca.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                        }

                        if (prosesPelangganAir)
                        {
                            await Utils.TrackProgress("master_pelanggan_air", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_pelanggan_air",
                                    queryPath: @"queries\bacameter\master_pelanggan_air.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_pelanggan_air_detail", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.BsbsConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_pelanggan_air_detail",
                                    queryPath: @"queries\bacameter\master_pelanggan_air_detail.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    });
                            });
                        }

                        if (prosesRekeningAir)
                        {
                            await RekeningAir(settings);
                        }
                    });
            }
            catch (Exception)
            {
                throw;
            }

            return 0;
        }

        private static async Task RekeningAir(Settings settings)
        {
            await Utils.TrackProgress("drd|rekening_air", async () =>
            {
                var lastId = 0;
                await Utils.MainConnectionWrapper(async (conn, trans) =>
                {
                    lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                });

                await Utils.BulkCopy(
                    sourceConnection: AppSettings.BsbsConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_air",
                    queryPath: @"queries\bacameter\drd.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                        { "@lastid", lastId },
                    });
            });

            await Utils.TrackProgress("drddetail|rekening_air", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.BsbsConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_air_detail",
                    queryPath: @"queries\bacameter\drd_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });
        }
    }
}
