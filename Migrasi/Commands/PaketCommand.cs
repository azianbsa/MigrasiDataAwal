using Dapper;
using Migrasi.Helpers;
using Spectre.Console;
using Spectre.Console.Cli;
using Sprache;

namespace Migrasi.Commands
{
    public class PaketCommand : AsyncCommand<PaketCommand.Settings>
    {
        public class Settings : CommandSettings
        {
            [CommandOption("-i|--idpdam")]
            public int? IdPdam { get; set; }

            [CommandOption("-n|--nama-paket")]
            public Paket? NamaPaket { get; set; }
        }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            settings.IdPdam ??= AnsiConsole.Ask<int>("ID PDAM:");
            settings.NamaPaket ??= AnsiConsole.Prompt(
                    new SelectionPrompt<Paket>()
                    .Title("Paket")
                    .AddChoices([Paket.Bacameter, Paket.Basic]));

            switch (settings.NamaPaket)
            {
                case Paket.Bacameter:
                    {
                        var periodeHMin4 = AnsiConsole.Ask<int>("Periode H-4 (yyyyMM):");

                        string? namaPdam = "";
                        await Utils.Client(async (conn, trans) =>
                        {
                            namaPdam = await conn.QueryFirstOrDefaultAsync<string>(@"SELECT namapdam FROM master_attribute_pdam WHERE idpdam=@idpdam", new { idpdam = settings.IdPdam }, trans);
                        });

                        AnsiConsole.Write(
                            new Table()
                            .AddColumn(new TableColumn("Setting"))
                            .AddColumn(new TableColumn("Value"))
                            .AddRow("Pdam", $"{settings.IdPdam} {namaPdam}")
                            .AddRow("Paket", settings.NamaPaket.ToString()!)
                            .AddRow("Periode", $"{DateTime.ParseExact(periodeHMin4.ToString(), "yyyyMM", null).AddMonths(0).ToString("yyyyMM")} - {DateTime.ParseExact(periodeHMin4.ToString(), "yyyyMM", null).AddMonths(3).ToString("yyyyMM")}")
                            .AddRow("DB Source", $"{AppSettings.DatabaseBacameter},{AppSettings.DatabaseBsbs}")
                            .AddRow("Host Target", AppSettings.Host)
                            .AddRow("Port Target", AppSettings.Port.ToString())
                            .AddRow("DB Target", AppSettings.Database)
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
                                    await Utils.TrackProgress("tambah idpelanggan", async () =>
                                    {
                                        await Utils.ClientBsbs(async (conn, trans) =>
                                        {
                                            var cek = await conn.QueryFirstOrDefaultAsync<int?>("SELECT 1 FROM information_schema.COLUMNS WHERE table_schema=@schema AND table_name='pelanggan' AND column_name='id'",
                                                new { schema = AppSettings.DatabaseBsbs }, trans);
                                            if (cek is null)
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\patches\tambah_field_id_tabel_pelanggan.sql");
                                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                            }
                                        });

                                        //ctx.Status("cek golongan");
                                        //await Utils.ClientBsbs(async (conn, trans) =>
                                        //{
                                        //    var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_golongan.sql");
                                        //    query = query.Replace("[table]", "pelanggan");
                                        //    await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        //});

                                        //ctx.Status("cek diameter");
                                        //await Utils.ClientBsbs(async (conn, trans) =>
                                        //{
                                        //    var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_diameter.sql");
                                        //    query = query.Replace("[table]", "pelanggan");
                                        //    await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        //});

                                        //ctx.Status("cek merek meter");
                                        //await Utils.ClientBsbs(async (conn, trans) =>
                                        //{
                                        //    var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_merek_meter.sql");
                                        //    await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        //});

                                        //ctx.Status("cek kelurahan");
                                        //await Utils.ClientBsbs(async (conn, trans) =>
                                        //{
                                        //    var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_kelurahan.sql");
                                        //    query = query.Replace("[table]", "pelanggan");
                                        //    await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        //});

                                        //ctx.Status("cek kolektif");
                                        //await Utils.ClientBsbs(async (conn, trans) =>
                                        //{
                                        //    var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_kolektif.sql");
                                        //    query = query.Replace("[table]", "pelanggan");
                                        //    await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        //});

                                        //ctx.Status("cek sumber air");
                                        //await Utils.ClientBsbs(async (conn, trans) =>
                                        //{
                                        //    var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_sumber_air.sql");
                                        //    await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        //});

                                        //ctx.Status("cek kondisi meter");
                                        //await Utils.ClientBsbs(async (conn, trans) =>
                                        //{
                                        //    var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_kondisi_meter.sql");
                                        //    await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        //});

                                        //ctx.Status("cek administrasi lain");
                                        //await Utils.ClientBsbs(async (conn, trans) =>
                                        //{
                                        //    var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_adm_lain.sql");
                                        //    query = query.Replace("[table]", "pelanggan");
                                        //    await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        //});

                                        //ctx.Status("cek pemeliharaan lain");
                                        //await Utils.ClientBsbs(async (conn, trans) =>
                                        //{
                                        //    var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_pem_lain.sql");
                                        //    query = query.Replace("[table]", "pelanggan");
                                        //    await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        //});

                                        //ctx.Status("cek retribusi lain");
                                        //await Utils.ClientBsbs(async (conn, trans) =>
                                        //{
                                        //    var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_ret_lain.sql");
                                        //    query = query.Replace("[table]", "pelanggan");
                                        //    await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        //});
                                    });
                                    await Utils.TrackProgress("master_attribute_flag", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            queryPath: @"Queries\bacameter\master_attribute_flag.sql",
                                            tableName: "master_attribute_flag",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_status", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_status",
                                            queryPath: @"Queries\bacameter\master_attribute_status.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_jenis_bangunan", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_jenis_bangunan",
                                            queryPath: @"Queries\bacameter\master_attribute_jenis_bangunan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_kepemilikan", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kepemilikan",
                                            queryPath: @"Queries\bacameter\master_attribute_kepemilikan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_pekerjaan", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_pekerjaan",
                                            queryPath: @"Queries\bacameter\master_attribute_pekerjaan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_peruntukan", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_peruntukan",
                                            queryPath: @"Queries\bacameter\master_attribute_peruntukan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_jenis_pipa", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_jenis_pipa",
                                            queryPath: @"Queries\bacameter\master_attribute_jenis_pipa.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_kwh", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kwh",
                                            queryPath: @"Queries\bacameter\master_attribute_kwh.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_tarif_golongan", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_golongan",
                                            queryPath: @"Queries\bacameter\master_tarif_golongan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_tarif_golongan_detail", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_golongan_detail",
                                            queryPath: @"Queries\bacameter\master_tarif_golongan_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_tarif_diameter", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_diameter",
                                            queryPath: @"Queries\bacameter\master_tarif_diameter.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_tarif_diameter_detail", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_diameter_detail",
                                            queryPath: @"Queries\bacameter\master_tarif_diameter_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_wilayah", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_wilayah",
                                            queryPath: @"Queries\bacameter\master_attribute_wilayah.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_area", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_area",
                                            queryPath: @"Queries\bacameter\master_attribute_area.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_rayon", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_rayon",
                                            queryPath: @"Queries\bacameter\master_attribute_rayon.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_blok", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_blok",
                                            queryPath: @"Queries\bacameter\master_attribute_blok.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_cabang", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_cabang",
                                            queryPath: @"Queries\bacameter\master_attribute_cabang.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_kecamatan", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kecamatan",
                                            queryPath: @"Queries\bacameter\master_attribute_kecamatan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_kelurahan", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kelurahan",
                                            queryPath: @"Queries\bacameter\master_attribute_kelurahan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_dma", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_dma",
                                            queryPath: @"Queries\bacameter\master_attribute_dma.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_dmz", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_dmz",
                                            queryPath: @"Queries\bacameter\master_attribute_dmz.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_tarif_administrasi_lain", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_administrasi_lain",
                                            queryPath: @"Queries\bacameter\master_tarif_administrasi_lain.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_tarif_pemeliharaan_lain", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_pemeliharaan_lain",
                                            queryPath: @"Queries\bacameter\master_tarif_pemeliharaan_lain.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_tarif_retribusi_lain", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_retribusi_lain",
                                            queryPath: @"Queries\bacameter\master_tarif_retribusi_lain.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_kolektif", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kolektif",
                                            queryPath: @"Queries\bacameter\master_attribute_kolektif.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_sumber_air", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_sumber_air",
                                            queryPath: @"Queries\bacameter\master_attribute_sumber_air.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_merek_meter", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_merek_meter",
                                            queryPath: @"Queries\bacameter\master_attribute_merek_meter.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_kondisi_meter", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kondisi_meter",
                                            queryPath: @"Queries\bacameter\master_attribute_kondisi_meter.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_kelainan", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBacameter,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kelainan",
                                            queryPath: @"Queries\master\master_attribute_kelainan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_petugas_baca", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBacameter,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_petugas_baca",
                                            queryPath: @"Queries\master\master_attribute_petugas_baca.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_periode", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_periode",
                                            queryPath: @"Queries\bacameter\master_periode.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_periode_billing", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_periode_billing",
                                            queryPath: @"Queries\bacameter\master_periode_billing.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });
                                    await Utils.TrackProgress("master_attribute_jadwal_baca", async () =>
                                    {
                                        await Utils.ClientBacameter(async (conn, trans) =>
                                        {
                                            var jadwalbaca = await conn.QueryAsync(
                                                sql: @"SELECT
                                                b.nama AS petugasbaca,
                                                c.koderayon
                                                FROM
                                                jadwalbaca a
                                                JOIN petugasbaca b ON a.idpetugas=b.idpetugas
                                                JOIN rayon c ON a.idrayon=c.idrayon",
                                                transaction: trans);
                                            if (jadwalbaca.Any())
                                            {
                                                await Utils.Client(async (conn, trans) =>
                                                {
                                                    List<dynamic> data = [];
                                                    var listPetugas = await conn.QueryAsync(
                                                        sql: @"SELECT idpetugasbaca,petugasbaca FROM master_attribute_petugas_baca WHERE idpdam=@idpdam",
                                                        param: new
                                                        {
                                                            idpdam = settings.IdPdam
                                                        },
                                                        transaction: trans);
                                                    var listRayon = await conn.QueryAsync(
                                                        sql: @"SELECT idrayon,koderayon FROM master_attribute_rayon WHERE idpdam=@idpdam",
                                                        param: new
                                                        {
                                                            idpdam = settings.IdPdam
                                                        },
                                                        transaction: trans);

                                                    int id = 1;
                                                    foreach (var item in jadwalbaca)
                                                    {
                                                        dynamic o = new
                                                        {
                                                            idpdam = settings.IdPdam,
                                                            idjadwalbaca = id++,
                                                            idpetugasbaca =
                                                                listPetugas
                                                                    .Where(s => s.petugasbaca.ToLower() == item.petugasbaca.ToLower())
                                                                    .Select(s => s.idpetugasbaca).FirstOrDefault(),
                                                            idrayon =
                                                                listRayon
                                                                    .Where(s => s.koderayon == item.koderayon)
                                                                    .Select(s => s.idrayon).FirstOrDefault()
                                                        };

                                                        if (o.idpetugasbaca != null && o.idrayon != null)
                                                        {
                                                            data.Add(o);
                                                        }
                                                    }

                                                    if (data.Count != 0)
                                                    {
                                                        await conn.ExecuteAsync(
                                                            sql: @"
                                                            REPLACE master_attribute_jadwal_baca (idpdam,idjadwalbaca,idpetugasbaca,idrayon)
                                                            VALUES (@idpdam,@idjadwalbaca,@idpetugasbaca,@idrayon)",
                                                            param: data,
                                                            transaction: trans);
                                                    }
                                                });
                                            }
                                        });
                                    });
                                    await Utils.TrackProgress("master_pelanggan_air", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_pelanggan_air",
                                            queryPath: @"Queries\bacameter\master_pelanggan_air.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            },
                                            placeholders: new()
                                            {
                                                { "[bacameter]", AppSettings.DatabaseBacameter }
                                            });
                                    });
                                    await Utils.TrackProgress("master_pelanggan_air_detail", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBsbs,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_pelanggan_air_detail",
                                            queryPath: @"Queries\bacameter\master_pelanggan_air_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });

                                    var dt = DateTime.ParseExact(periodeHMin4.ToString(), "yyyyMM", null);
                                    for (int i = 0; i < 4; i++)
                                    {
                                        var periode = dt.AddMonths(i).ToString("yyyyMM");

                                        //await Utils.TrackProgress($"cleanup data drd{periode}", async () =>
                                        //{
                                        //    ctx.Status("cek golongan");
                                        //    await Utils.ClientBsbs(async (conn, trans) =>
                                        //    {
                                        //        var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_golongan.sql");
                                        //        query = query.Replace("[table]", $"drd{periode}");
                                        //        await conn.ExecuteAsync(query, transaction: trans);
                                        //    });

                                        //    ctx.Status("cek diameter");
                                        //    await Utils.ClientBsbs(async (conn, trans) =>
                                        //    {
                                        //        var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_diameter.sql");
                                        //        query = query.Replace("[table]", $"drd{periode}");
                                        //        await conn.ExecuteAsync(query, transaction: trans);
                                        //    });

                                        //    ctx.Status("cek kelurahan");
                                        //    await Utils.ClientBsbs(async (conn, trans) =>
                                        //    {
                                        //        var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_kelurahan.sql");
                                        //        query = query.Replace("[table]", $"drd{periode}");
                                        //        await conn.ExecuteAsync(query, transaction: trans);
                                        //    });

                                        //    ctx.Status("cek kolektif");
                                        //    await Utils.ClientBsbs(async (conn, trans) =>
                                        //    {
                                        //        var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_kolektif.sql");
                                        //        query = query.Replace("[table]", $"drd{periode}");
                                        //        await conn.ExecuteAsync(query, transaction: trans);
                                        //    });

                                        //    ctx.Status("cek administrasi lain");
                                        //    await Utils.ClientBsbs(async (conn, trans) =>
                                        //    {
                                        //        var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_adm_lain.sql");
                                        //        query = query.Replace("[table]", $"drd{periode}");
                                        //        await conn.ExecuteAsync(query, transaction: trans);
                                        //    });

                                        //    ctx.Status("cek pemeliharaan lain");
                                        //    await Utils.ClientBsbs(async (conn, trans) =>
                                        //    {
                                        //        var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_pem_lain.sql");
                                        //        query = query.Replace("[table]", $"drd{periode}");
                                        //        await conn.ExecuteAsync(query, transaction: trans);
                                        //    });

                                        //    ctx.Status("cek retribusi lain");
                                        //    await Utils.ClientBsbs(async (conn, trans) =>
                                        //    {
                                        //        var query = await File.ReadAllTextAsync(@"Queries\patches\data_cleanup_ret_lain.sql");
                                        //        query = query.Replace("[table]", $"drd{periode}");
                                        //        await conn.ExecuteAsync(query, transaction: trans);
                                        //    });
                                        //});

                                        await Utils.TrackProgress($"drd{periode}", async () =>
                                        {
                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringBsbs,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air",
                                                queryPath: @"Queries\bacameter\drd.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam }
                                                },
                                                placeholders: new()
                                                {
                                                    { "[tahunbulan]", periode },
                                                    { "[bacameter]", AppSettings.DatabaseBacameter },
                                                });

                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringBsbs,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air_detail",
                                                queryPath: @"Queries\bacameter\drd_detail.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam }
                                                },
                                                placeholders: new()
                                                {
                                                    { "[tahunbulan]", periode }
                                                });
                                        });
                                    }
                                });

                            AnsiConsole.MarkupLine("");
                            AnsiConsole.MarkupLine($"[bold green]Migrasi data bacameter finish.[/]");
                        }
                        catch (Exception)
                        {
                            throw;
                        }

                        break;
                    }
                case Paket.Basic:
                    {
                        string? namaPdam = "";
                        await Utils.Client(async (conn, trans) =>
                        {
                            namaPdam = await conn.QueryFirstOrDefaultAsync<string>(@"SELECT namapdam FROM master_attribute_pdam WHERE idpdam=@idpdam", new { idpdam = settings.IdPdam }, trans);
                        });

                        AnsiConsole.Write(
                            new Table()
                            .AddColumn(new TableColumn("Setting"))
                            .AddColumn(new TableColumn("Value"))
                            .AddRow("Pdam", $"{settings.IdPdam} {namaPdam}")
                            .AddRow("Paket", settings.NamaPaket.ToString()!)
                            .AddRow("DB Bacameter V4", AppSettings.DatabaseBacameter)
                            .AddRow("DB Billing V4", AppSettings.DatabaseBsbs)
                            .AddRow("DB Loket V4", AppSettings.DatabaseLoket)
                            .AddRow("Host Target", AppSettings.Host)
                            .AddRow("Port Target", AppSettings.Port.ToString())
                            .AddRow("DB Target", AppSettings.Database)
                            .AddRow("Environment", AppSettings.Environment.ToString()));

                        if (!Utils.ConfirmationPrompt("Yakin untuk melanjutkan?"))
                        {
                            return 0;
                        }

                        try
                        {
                            await Utils.Client(async (conn, trans) =>
                            {
                                await conn.ExecuteAsync(@"
                                    SET GLOBAL foreign_key_checks = 0;
                                    SET GLOBAL innodb_flush_log_at_trx_commit = 2;
                                    ", transaction: trans);
                            });

                            await AnsiConsole.Status()
                                .StartAsync("Sedang diproses...", async ctx =>
                                {
                                    await MasterData(settings);
                                    await JenisNonair(settings);
                                    await TipePermohonan(settings);
                                    await PaketMaterial(settings);
                                    await PaketOngkos(settings);
                                    await PaketRab(settings);
                                    await Report(settings);

                                    await Utils.TrackProgress("master_pelanggan_air", async () =>
                                    {
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_pelanggan_air",
                                            queryPath: @"Queries\master_pelanggan_air.sql",
                                            parameters: new()
                                            {
                                                    { "@idpdam", settings.IdPdam }
                                            },
                                            placeholders: new()
                                            {
                                                    { "[bacameter]", AppSettings.DatabaseBacameter },
                                                    { "[bsbs]", AppSettings.DatabaseBsbs },
                                            });
                                    });

                                    await Utils.TrackProgress("master_pelanggan_air_detail", async () =>
                                    {
                                        await Utils.Client(async (conn, trans) =>
                                        {
                                            await conn.ExecuteAsync(@"
                                                ALTER TABLE master_pelanggan_air_detail
                                                 CHANGE alamatpemilik alamatpemilik VARCHAR (250) CHARSET latin1 COLLATE latin1_swedish_ci NULL", transaction: trans);
                                        });

                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_pelanggan_air_detail",
                                            queryPath: @"Queries\master_pelanggan_air_detail.sql",
                                            parameters: new()
                                            {
                                                    { "@idpdam", settings.IdPdam }
                                            },
                                            placeholders: new()
                                            {
                                                    { "[bsbs]", AppSettings.DatabaseBsbs }
                                            });
                                    });

                                    await Utils.TrackProgress("piutang", async () =>
                                    {
                                        var lastId = 0;
                                        await Utils.Client(async (conn, trans) =>
                                        {
                                            lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                                        });

                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "rekening_air",
                                            queryPath: @"Queries\piutang\piutang.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                                { "@lastid", lastId },
                                            },
                                            placeholders: new()
                                            {
                                                { "[bacameter]", AppSettings.DatabaseBacameter },
                                                { "[bsbs]", AppSettings.DatabaseBsbs },
                                            });

                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "rekening_air_detail",
                                            queryPath: @"Queries\piutang\piutang_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                            },
                                            placeholders: new()
                                            {
                                                { "[bsbs]", AppSettings.DatabaseBsbs },
                                            });
                                    });

                                    await Utils.TrackProgress("bayar tahun", async () =>
                                    {
                                        IEnumerable<string?> bayarTahun = [];
                                        await Utils.ClientLoket(async (conn, trans) =>
                                        {
                                            bayarTahun = await conn.QueryAsync<string?>(
                                                sql: @"SELECT RIGHT(table_name, 4) FROM information_schema.TABLES WHERE table_schema=@table_schema AND table_name RLIKE 'bayar[0-9]{4}'",
                                                param: new { table_schema = AppSettings.DatabaseLoket },
                                                transaction: trans);
                                        });

                                        foreach (var tahun in bayarTahun)
                                        {
                                            await Utils.TrackProgress($"bayar{tahun}", async () =>
                                            {
                                                IEnumerable<int>? listPeriode = [];
                                                await Utils.ClientLoket(async (conn, trans) =>
                                                {
                                                    listPeriode = await conn.QueryAsync<int>(sql: $@"SELECT periode FROM bayar{tahun} GROUP BY periode", transaction: trans);
                                                });

                                                foreach (var periode in listPeriode)
                                                {
                                                    await Utils.TrackProgress($"bayar{tahun}-{periode}|rekening_air", async () =>
                                                    {
                                                        var lastId = 0;
                                                        await Utils.Client(async (conn, trans) =>
                                                        {
                                                            lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                                                        });

                                                        await Utils.BulkCopy(
                                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                                            tConnectionStr: AppSettings.ConnectionString,
                                                            tableName: "rekening_air",
                                                            queryPath: @"Queries\bayar\bayar.sql",
                                                            parameters: new()
                                                            {
                                                                { "@idpdam", settings.IdPdam },
                                                                { "@lastid", lastId },
                                                                { "@periode", periode },
                                                            },
                                                            placeholders: new()
                                                            {
                                                                { "[table]", $"bayar{tahun}" },
                                                                { "[bacameter]", AppSettings.DatabaseBacameter },
                                                                { "[bsbs]", AppSettings.DatabaseBsbs },
                                                            });
                                                    }, usingStopwatch: true);

                                                    await Utils.TrackProgress($"bayar{tahun}-{periode}|rekening_air_detail", async () =>
                                                    {
                                                        await Utils.BulkCopy(
                                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                                            tConnectionStr: AppSettings.ConnectionString,
                                                            tableName: "rekening_air_detail",
                                                            queryPath: @"Queries\bayar\bayar_detail.sql",
                                                            parameters: new()
                                                            {
                                                                { "@idpdam", settings.IdPdam },
                                                                { "@periode", periode },
                                                            },
                                                            placeholders: new()
                                                            {
                                                                { "[table]", $"bayar{tahun}" },
                                                                { "[bsbs]", AppSettings.DatabaseBsbs },
                                                            });
                                                    }, usingStopwatch: true);

                                                    await Utils.TrackProgress($"bayar{tahun}-{periode}|rekening_air_transaksi", async () =>
                                                    {
                                                        await Utils.BulkCopy(
                                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                                            tConnectionStr: AppSettings.ConnectionString,
                                                            tableName: "rekening_air_transaksi",
                                                            queryPath: @"Queries\bayar\bayar_transaksi.sql",
                                                            parameters: new()
                                                            {
                                                                { "@idpdam", settings.IdPdam },
                                                                { "@periode", periode },
                                                            },
                                                            placeholders: new()
                                                            {
                                                                { "[table]", $"bayar{tahun}" },
                                                                { "[bacameter]", AppSettings.DatabaseBacameter },
                                                                { "[bsbs]", AppSettings.DatabaseBsbs },
                                                            });
                                                    }, usingStopwatch: true);
                                                }
                                            });
                                        }
                                    });

                                    await Utils.TrackProgress("bayar", async () =>
                                    {
                                        IEnumerable<int>? listPeriode = [];
                                        await Utils.ClientLoket(async (conn, trans) =>
                                        {
                                            listPeriode = await conn.QueryAsync<int>(sql: $@"SELECT periode FROM bayar GROUP BY periode", transaction: trans);
                                        });

                                        foreach (var periode in listPeriode)
                                        {
                                            await Utils.TrackProgress($"bayar-{periode}|rekening_air", async () =>
                                            {
                                                var lastId = 0;
                                                await Utils.Client(async (conn, trans) =>
                                                {
                                                    lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                                                });

                                                await Utils.BulkCopy(
                                                    sConnectionStr: AppSettings.ConnectionStringLoket,
                                                    tConnectionStr: AppSettings.ConnectionString,
                                                    tableName: "rekening_air",
                                                    queryPath: @"Queries\bayar\bayar.sql",
                                                    parameters: new()
                                                    {
                                                        { "@idpdam", settings.IdPdam },
                                                        { "@lastid", lastId },
                                                        { "@periode", periode },
                                                    },
                                                    placeholders: new()
                                                    {
                                                        { "[table]", "bayar" },
                                                        { "[bacameter]", AppSettings.DatabaseBacameter },
                                                        { "[bsbs]", AppSettings.DatabaseBsbs },
                                                    });
                                            }, usingStopwatch: true);

                                            await Utils.TrackProgress($"bayar-{periode}|rekening_air_detail", async () =>
                                            {
                                                await Utils.BulkCopy(
                                                    sConnectionStr: AppSettings.ConnectionStringLoket,
                                                    tConnectionStr: AppSettings.ConnectionString,
                                                    tableName: "rekening_air_detail",
                                                    queryPath: @"Queries\bayar\bayar_detail.sql",
                                                    parameters: new()
                                                    {
                                                        { "@idpdam", settings.IdPdam },
                                                        { "@periode", periode },
                                                    },
                                                    placeholders: new()
                                                    {
                                                        { "[table]", "bayar" },
                                                        { "[bsbs]", AppSettings.DatabaseBsbs },
                                                    });
                                            }, usingStopwatch: true);

                                            await Utils.TrackProgress($"bayar-{periode}|rekening_air_transaksi", async () =>
                                            {
                                                await Utils.BulkCopy(
                                                    sConnectionStr: AppSettings.ConnectionStringLoket,
                                                    tConnectionStr: AppSettings.ConnectionString,
                                                    tableName: "rekening_air_transaksi",
                                                    queryPath: @"Queries\bayar\bayar_transaksi.sql",
                                                    parameters: new()
                                                    {
                                                        { "@idpdam", settings.IdPdam },
                                                        { "@periode", periode },
                                                    },
                                                    placeholders: new()
                                                    {
                                                        { "[table]", "bayar" },
                                                        { "[bacameter]", AppSettings.DatabaseBacameter },
                                                        { "[bsbs]", AppSettings.DatabaseBsbs },
                                                    });
                                            }, usingStopwatch: true);
                                        }
                                    });

                                    await Utils.TrackProgress("nonair tahun", async () =>
                                    {
                                        IEnumerable<string?> nonairTahun = [];
                                        await Utils.ClientLoket(async (conn, trans) =>
                                        {
                                            nonairTahun = await conn.QueryAsync<string?>(
                                                sql: @"SELECT RIGHT(table_name, 4) FROM information_schema.TABLES WHERE table_schema=@table_schema AND table_name RLIKE 'nonair[0-9]{4}'",
                                                param: new
                                                {
                                                    table_schema = AppSettings.DatabaseLoket
                                                },
                                                transaction: trans);
                                        });

                                        foreach (var tahun in nonairTahun)
                                        {
                                            await Utils.TrackProgress($"nonair{tahun}", async () =>
                                            {
                                                IEnumerable<int>? listPeriode = [];
                                                await Utils.ClientLoket(async (conn, trans) =>
                                                {
                                                    listPeriode = await conn.QueryAsync<int>(
                                                        sql: @"SELECT a.periode FROM (SELECT CASE WHEN periode IS NULL OR periode='' THEN -1 ELSE periode END AS periode FROM nonair GROUP BY periode) a GROUP BY a.periode",
                                                        transaction: trans);
                                                });

                                                IEnumerable<dynamic>? jenis = [];

                                                await Utils.Client(async (conn, trans) =>
                                                {
                                                    await conn.ExecuteAsync(
                                                        sql: @"
                                                        ALTER TABLE rekening_nonair_transaksi
                                                        CHANGE keterangan keterangan VARCHAR (1000) CHARSET latin1 COLLATE latin1_swedish_ci NULL",
                                                        transaction: trans);

                                                    jenis = await conn.QueryAsync(
                                                        sql: @"SELECT idjenisnonair,kodejenisnonair FROM master_attribute_jenis_nonair WHERE idpdam=@idpdam AND flaghapus=0",
                                                        param: new
                                                        {
                                                            idpdam = settings.IdPdam
                                                        },
                                                        transaction: trans);
                                                });

                                                await Utils.ClientLoket(async (conn, trans) =>
                                                {
                                                    if (jenis != null)
                                                    {
                                                        await conn.ExecuteAsync(
                                                            sql: @"
                                                            DROP TABLE IF EXISTS __tmp_jenisnonair;

                                                            CREATE TABLE __tmp_jenisnonair (
                                                            idjenisnonair INT,
                                                            kodejenisnonair VARCHAR(50)
                                                            )",
                                                            transaction: trans);

                                                        await conn.ExecuteAsync(
                                                            sql: @"
                                                            INSERT INTO __tmp_jenisnonair
                                                            VALUES (@idjenisnonair,@kodejenisnonair)",
                                                            param: jenis,
                                                            transaction: trans);
                                                    }
                                                });

                                                foreach (var periode in listPeriode)
                                                {
                                                    await Utils.TrackProgress($"nonair{tahun}-{periode}|rekening_nonair", async () =>
                                                    {
                                                        await Utils.BulkCopy(
                                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                                            tConnectionStr: AppSettings.ConnectionString,
                                                            tableName: "rekening_nonair",
                                                            queryPath: @"Queries\nonair\nonair.sql",
                                                            parameters: new()
                                                            {
                                                                { "@idpdam", settings.IdPdam },
                                                                { "@periode", periode },
                                                            },
                                                            placeholders: new()
                                                            {
                                                                { "[table]", $"nonair{tahun}" },
                                                                { "[bsbs]", AppSettings.DatabaseBsbs },
                                                            });
                                                    }, usingStopwatch: true);

                                                    await Utils.TrackProgress($"nonair{tahun}-{periode}|rekening_nonair_detail", async () =>
                                                    {
                                                        await Utils.BulkCopy(
                                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                                            tConnectionStr: AppSettings.ConnectionString,
                                                            tableName: "rekening_nonair_detail",
                                                            queryPath: @"Queries\nonair\nonair_detail.sql",
                                                            parameters: new()
                                                            {
                                                                { "@idpdam", settings.IdPdam },
                                                                { "@periode", periode },
                                                            },
                                                            placeholders: new()
                                                            {
                                                                { "[table]", $"nonair{tahun}" },
                                                            });
                                                    }, usingStopwatch: true);

                                                    await Utils.TrackProgress($"nonair{tahun}-{periode}|rekening_nonair_transaksi", async () =>
                                                    {
                                                        await Utils.BulkCopy(
                                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                                            tConnectionStr: AppSettings.ConnectionString,
                                                            tableName: "rekening_nonair_transaksi",
                                                            queryPath: @"Queries\nonair\nonair_transaksi.sql",
                                                            parameters: new()
                                                            {
                                                                { "@idpdam", settings.IdPdam },
                                                                { "@periode", periode },
                                                            },
                                                            placeholders: new()
                                                            {
                                                                { "[table]", $"nonair{tahun}" },
                                                                { "[bacameter]", AppSettings.DatabaseBacameter },
                                                                { "[bsbs]", AppSettings.DatabaseBsbs },
                                                            });
                                                    }, usingStopwatch: true);
                                                }
                                            });
                                        }

                                        await Utils.ClientLoket(async (conn, trans) =>
                                        {
                                            await conn.ExecuteAsync(sql: @"DROP TABLE IF EXISTS __tmp_jenisnonair", transaction: trans);
                                        });
                                    });

                                    await Utils.TrackProgress("nonair", async () =>
                                    {
                                        IEnumerable<int>? listPeriode = [];
                                        await Utils.ClientLoket(async (conn, trans) =>
                                        {
                                            listPeriode = await conn.QueryAsync<int>(
                                                sql: @"SELECT a.periode FROM (SELECT CASE WHEN periode IS NULL OR periode='' THEN -1 ELSE periode END AS periode FROM nonair GROUP BY periode) a GROUP BY a.periode",
                                                transaction: trans);
                                        });

                                        IEnumerable<dynamic>? jenis = [];

                                        await Utils.Client(async (conn, trans) =>
                                        {
                                            await conn.ExecuteAsync(
                                                sql: @"
                                                ALTER TABLE rekening_nonair_transaksi
                                                CHANGE keterangan keterangan VARCHAR (1000) CHARSET latin1 COLLATE latin1_swedish_ci NULL",
                                                transaction: trans);

                                            jenis = await conn.QueryAsync(
                                                sql: @"SELECT idjenisnonair,kodejenisnonair FROM master_attribute_jenis_nonair WHERE idpdam=@idpdam AND flaghapus=0",
                                                param: new
                                                {
                                                    idpdam = settings.IdPdam
                                                },
                                                transaction: trans);
                                        });

                                        await Utils.ClientLoket(async (conn, trans) =>
                                        {
                                            if (jenis != null)
                                            {
                                                await conn.ExecuteAsync(
                                                    sql: @"
                                                    DROP TABLE IF EXISTS __tmp_jenisnonair;

                                                    CREATE TABLE __tmp_jenisnonair (
                                                    idjenisnonair INT,
                                                    kodejenisnonair VARCHAR(50)
                                                    )",
                                                    transaction: trans);

                                                await conn.ExecuteAsync(
                                                    sql: @"
                                                    INSERT INTO __tmp_jenisnonair
                                                    VALUES (@idjenisnonair,@kodejenisnonair)",
                                                    param: jenis,
                                                 transaction: trans);
                                            }
                                        });

                                        foreach (var periode in listPeriode)
                                        {
                                            await Utils.TrackProgress($"nonair-{periode}|rekening_nonair", async () =>
                                            {
                                                await Utils.BulkCopy(
                                                    sConnectionStr: AppSettings.ConnectionStringLoket,
                                                    tConnectionStr: AppSettings.ConnectionString,
                                                    tableName: "rekening_nonair",
                                                    queryPath: @"Queries\nonair\nonair.sql",
                                                    parameters: new()
                                                    {
                                                        { "@idpdam", settings.IdPdam },
                                                        { "@periode", periode },
                                                    },
                                                    placeholders: new()
                                                    {
                                                        { "[table]", "nonair" },
                                                        { "[bsbs]", AppSettings.DatabaseBsbs },
                                                    });
                                            }, usingStopwatch: true);

                                            await Utils.TrackProgress($"nonair-{periode}|rekening_nonair_detail", async () =>
                                            {
                                                await Utils.BulkCopy(
                                                    sConnectionStr: AppSettings.ConnectionStringLoket,
                                                    tConnectionStr: AppSettings.ConnectionString,
                                                    tableName: "rekening_nonair_detail",
                                                    queryPath: @"Queries\nonair\nonair_detail.sql",
                                                    parameters: new()
                                                    {
                                                        { "@idpdam", settings.IdPdam },
                                                        { "@periode", periode },
                                                    },
                                                    placeholders: new()
                                                    {
                                                        { "[table]", "nonair" },
                                                    });
                                            }, usingStopwatch: true);

                                            await Utils.TrackProgress($"nonair-{periode}|rekening_nonair_transaksi", async () =>
                                            {
                                                await Utils.BulkCopy(
                                                    sConnectionStr: AppSettings.ConnectionStringLoket,
                                                    tConnectionStr: AppSettings.ConnectionString,
                                                    tableName: "rekening_nonair_transaksi",
                                                    queryPath: @"Queries\nonair\nonair_transaksi.sql",
                                                    parameters: new()
                                                    {
                                                        { "@idpdam", settings.IdPdam },
                                                        { "@periode", periode },
                                                    },
                                                    placeholders: new()
                                                    {
                                                        { "[table]", "nonair" },
                                                        { "[bacameter]", AppSettings.DatabaseBacameter },
                                                        { "[bsbs]", AppSettings.DatabaseBsbs },
                                                    });
                                            }, usingStopwatch: true);
                                        }

                                        await Utils.ClientLoket(async (conn, trans) =>
                                        {
                                            await conn.ExecuteAsync(sql: @"DROP TABLE IF EXISTS __tmp_jenisnonair", transaction: trans);
                                        });
                                    });

                                    await Utils.TrackProgress("angsuran air", async () =>
                                    {
                                        IEnumerable<int>? listPeriode = [];
                                        await Utils.ClientLoket(async (conn, trans) =>
                                        {
                                            listPeriode = await conn.QueryAsync<int>(@"SELECT periode FROM piutang WHERE flagangsur=1 GROUP BY periode", transaction: trans);
                                        });

                                        await Utils.ClientLoket(async (conn, trans) =>
                                        {
                                            await conn.ExecuteAsync(
                                                sql: @"
                                                DROP TABLE IF EXISTS __tmp_angsuranair;

                                                CREATE TABLE __tmp_angsuranair AS
                                                SELECT b.id AS idangsuran,b.noangsuran,CONCAT(a.periode,'.',a.nomor)kode,b.flaglunas
                                                FROM detailangsuran a
                                                JOIN daftarangsuran b ON b.noangsuran=a.noangsuran
                                                WHERE b.keperluan='JNS-36'
                                                GROUP BY a.periode,a.nomor ORDER BY a.nomor,a.periode",
                                                transaction: trans);
                                        });

                                        await Utils.TrackProgress($"angsuran air piutang|rekening_air", async () =>
                                        {
                                            var lastId = 0;
                                            await Utils.Client(async (conn, trans) =>
                                            {
                                                lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                                            });

                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringLoket,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air",
                                                queryPath: @"Queries\angsuran_air\piutang_rekening_air.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam },
                                                    { "@lastid", lastId },
                                                },
                                                placeholders: new()
                                                {
                                                    { "[bacameter]", AppSettings.DatabaseBacameter },
                                                    { "[bsbs]", AppSettings.DatabaseBsbs },
                                                });
                                        }, usingStopwatch: true);

                                        await Utils.TrackProgress($"angsuran air piutang|rekening_air_detail", async () =>
                                        {
                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringLoket,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air_detail",
                                                queryPath: @"Queries\angsuran_air\piutang_rekening_air_detail.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam },
                                                },
                                                placeholders: new()
                                                {
                                                    { "[bsbs]", AppSettings.DatabaseBsbs },
                                                });
                                        }, usingStopwatch: true);

                                        await Utils.TrackProgress($"angsuran air bayar|rekening_air", async () =>
                                        {
                                            var lastId = 0;
                                            await Utils.Client(async (conn, trans) =>
                                            {
                                                lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                                            });

                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringLoket,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air",
                                                queryPath: @"Queries\angsuran_air\bayar_rekening_air.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam },
                                                    { "@lastid", lastId },
                                                },
                                                placeholders: new()
                                                {
                                                    { "[bacameter]", AppSettings.DatabaseBacameter },
                                                    { "[bsbs]", AppSettings.DatabaseBsbs },
                                                });
                                        }, usingStopwatch: true);

                                        await Utils.TrackProgress($"angsuran air bayar|rekening_air_detail", async () =>
                                        {
                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringLoket,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air_detail",
                                                queryPath: @"Queries\angsuran_air\bayar_rekening_air_detail.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam },
                                                },
                                                placeholders: new()
                                                {
                                                    { "[bsbs]", AppSettings.DatabaseBsbs },
                                                });
                                        }, usingStopwatch: true);

                                        await Utils.TrackProgress($"angsuran air|rekening_air_angsuran", async () =>
                                        {
                                            var lastIdAngsuran = 0;
                                            var jnsNonair = 0;
                                            await Utils.Client(async (conn, trans) =>
                                            {
                                                lastIdAngsuran = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idangsuran),0) FROM rekening_air_angsuran", transaction: trans);
                                                jnsNonair = await conn.QueryFirstOrDefaultAsync<int>($"SELECT idjenisnonair FROM master_attribute_jenis_nonair WHERE idpdam = {settings.IdPdam} AND kodejenisnonair = 'JNS-36' AND flaghapus = 0", transaction: trans);
                                            });

                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringLoket,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air_angsuran",
                                                queryPath: @"Queries\angsuran_air\rekening_air_angsuran.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam },
                                                    { "@jnsnonair", jnsNonair },
                                                },
                                                placeholders: new()
                                                {
                                                    { "[bacameter]", AppSettings.DatabaseBacameter },
                                                    { "[bsbs]", AppSettings.DatabaseBsbs },
                                                });
                                        }, usingStopwatch: true);

                                        await Utils.TrackProgress($"angsuran air|rekening_air_angsuran_detail", async () =>
                                        {
                                            var lastIdAngsuranDetail = 0;
                                            await Utils.Client(async (conn, trans) =>
                                            {
                                                lastIdAngsuranDetail = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(id),0) FROM rekening_air_angsuran_detail", transaction: trans);
                                            });

                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringLoket,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air_angsuran_detail",
                                                queryPath: @"Queries\angsuran_air\rekening_air_angsuran_detail.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam },
                                                },
                                                placeholders: new()
                                                {
                                                    { "[bacameter]", AppSettings.DatabaseBacameter },
                                                    { "[bsbs]", AppSettings.DatabaseBsbs },
                                                });
                                        }, usingStopwatch: true);
                                    });

                                    await Utils.TrackProgress("angsuran nonair", async () =>
                                    {
                                        IEnumerable<dynamic>? jenis = [];
                                        await Utils.Client(async (conn, trans) =>
                                        {
                                            jenis = await conn.QueryAsync(
                                                sql: @"SELECT idjenisnonair,kodejenisnonair FROM master_attribute_jenis_nonair WHERE idpdam=@idpdam AND flaghapus=0",
                                                param: new
                                                {
                                                    idpdam = settings.IdPdam
                                                },
                                                transaction: trans);
                                        });

                                        await Utils.ClientLoket(async (conn, trans) =>
                                        {
                                            await conn.ExecuteAsync(
                                                sql: @"
                                                DROP TABLE IF EXISTS __tmp_nonair;
                                                CREATE TABLE __tmp_nonair AS
                                                SELECT a.id AS idangsuran,a.jumlahtermin,a.noangsuran AS noangsuran1,b.*
                                                FROM `daftarangsuran` a
                                                LEFT JOIN nonair b ON b.`urutan`=a.`urutan_nonair`
                                                WHERE a.`keperluan`<>'JNS-36'",
                                                transaction: trans);
                                            
                                            if (jenis != null)
                                            {
                                                await conn.ExecuteAsync(
                                                    sql: @"
                                                    DROP TABLE IF EXISTS __tmp_jenisnonair;

                                                    CREATE TABLE __tmp_jenisnonair (
                                                    idjenisnonair INT,
                                                    kodejenisnonair VARCHAR(50))",
                                                    transaction: trans);

                                                await conn.ExecuteAsync(
                                                    sql: @"
                                                    INSERT INTO __tmp_jenisnonair
                                                    VALUES (@idjenisnonair,@kodejenisnonair)", param: jenis, transaction: trans);
                                            }
                                        });

                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "rekening_nonair",
                                            queryPath: @"Queries\angsuran_nonair\nonair.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                            },
                                            placeholders: new()
                                            {
                                                { "[bsbs]", AppSettings.DatabaseBsbs },
                                            });

                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "rekening_nonair_detail",
                                            queryPath: @"Queries\angsuran_nonair\nonair_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                            });

                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "rekening_nonair_angsuran",
                                            queryPath: @"Queries\angsuran_nonair\nonair_angsuran.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                            },
                                            placeholders: new()
                                            {
                                                { "[bacameter]", AppSettings.DatabaseBacameter },
                                                { "[bsbs]", AppSettings.DatabaseBsbs },
                                            });

                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "rekening_nonair_angsuran_detail",
                                            queryPath: @"Queries\angsuran_nonair\nonair_angsuran_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                            },
                                            placeholders: new()
                                            {
                                                { "[bacameter]", AppSettings.DatabaseBacameter },
                                                { "[bsbs]", AppSettings.DatabaseBsbs },
                                            });

                                        await Utils.ClientLoket(async (conn, trans) =>
                                        {
                                            await conn.ExecuteAsync(
                                                sql: @"
                                                DROP TABLE IF EXISTS __tmp_nonair;
                                                DROP TABLE IF EXISTS __tmp_jenisnonair;",
                                                transaction: trans);
                                        });
                                    });

                                    await Utils.TrackProgress("pengaduan pelanggan air", async () =>
                                    {
                                        await Pengaduan(settings);
                                    });

                                    await Utils.TrackProgress("balik nama", async () =>
                                    {
                                        await BalikNama(settings);
                                    });

                                    await Utils.TrackProgress("rubah golongan", async () =>
                                    {
                                        await RubahGolongan(settings);
                                    });

                                    await Utils.TrackProgress("rubah rayon", async () =>
                                    {
                                        await RubahRayon(settings);
                                    });

                                    await Utils.TrackProgress("sambung kembali", async () =>
                                    {
                                        await SambungKembali(settings);
                                    });

                                    await Utils.TrackProgress("buka segel", async () =>
                                    {
                                        await BukaSegel(settings);
                                    });

                                    await Utils.TrackProgress("sambung baru", async () =>
                                    {
                                        await SambungBaru(settings);
                                    });

                                    await Utils.TrackProgress("koreksi rekair", async () =>
                                    {
                                        await KoreksiRekair(settings);
                                    });

                                    await Utils.TrackProgress("rotasimeter", async () =>
                                    {
                                        await Rotasimeter(settings);
                                    });

                                    await Utils.TrackProgress("rotasimeter nonrutin", async () =>
                                    {
                                        await RotasimeterNonrutin(settings);
                                    });
                                });

                            AnsiConsole.MarkupLine("");
                            AnsiConsole.MarkupLine($"[bold green]Migrasi data basic finish.[/]");
                        }
                        catch (Exception)
                        {
                            throw;
                        }
                        finally
                        {
                            await Utils.Client(async (conn, trans) =>
                            {
                                await conn.ExecuteAsync(@"
                                    SET GLOBAL foreign_key_checks = 1;
                                    SET GLOBAL innodb_flush_log_at_trx_commit = 1;
                                    ", transaction: trans);
                            });
                        }

                        break;
                    }
                default:
                    break;
            }

            return 0;
        }

        private async Task Report(Settings settings)
        {
            await Utils.TrackProgress("master_attribute_label_report", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_label_report",
                    queryPath: @"Queries\master\report\master_attribute_label_report.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_report_maingroup", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_report_maingroup",
                    queryPath: @"Queries\master\report\master_report_maingroup.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_report_subgroup", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_report_subgroup",
                    queryPath: @"Queries\master\report\master_report_subgroup.sql");
            });

            await Utils.TrackProgress("report_api", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "report_api",
                    queryPath: @"Queries\master\report\report_api.sql");
            });

            await Utils.TrackProgress("report_models", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "report_models",
                    queryPath: @"Queries\master\report\report_models.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("report_model_sources", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "report_model_sources",
                    queryPath: @"Queries\master\report\report_model_sources.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("report_model_sorts", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "report_model_sorts",
                    queryPath: @"Queries\master\report\report_model_sorts.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("report_model_props", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "report_model_props",
                    queryPath: @"Queries\master\report\report_model_props.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("report_model_params", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "report_model_params",
                    queryPath: @"Queries\master\report\report_model_params.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("report_filter_custom", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "report_filter_custom",
                    queryPath: @"Queries\master\report\report_filter_custom.sql");
            });

            await Utils.TrackProgress("report_filter_custom_detail", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "report_filter_custom_detail",
                    queryPath: @"Queries\master\report\report_filter_custom_detail.sql");
            });
        }

        private async Task PaketRab(Settings settings)
        {
            await Utils.TrackProgress("master_attribute_paket", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_paket",
                    queryPath: @"Queries\master\master_attribute_paket.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });
        }

        private async Task PaketOngkos(Settings settings)
        {
            await Utils.TrackProgress("master_attribute_ongkos", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_ongkos",
                    queryPath: @"Queries\master\paket_ongkos\master_attribute_ongkos.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_ongkos_paket", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_ongkos_paket",
                    queryPath: @"Queries\master\paket_ongkos\master_attribute_ongkos_paket.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_ongkos_paket_detail", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_ongkos_paket_detail",
                    queryPath: @"Queries\master\paket_ongkos\master_attribute_ongkos_paket_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });
        }

        private async Task PaketMaterial(Settings settings)
        {
            await Utils.TrackProgress("master_attribute_material", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_material",
                    queryPath: @"Queries\master\paket_material\master_attribute_material.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_material_paket", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_material_paket",
                    queryPath: @"Queries\master\paket_material\master_attribute_material_paket.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_material_paket_detail", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_material_paket_detail",
                    queryPath: @"Queries\master\paket_material\master_attribute_material_paket_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });
        }

        private async Task TipePermohonan(Settings settings)
        {
            await Utils.TrackProgress("master_attribute_tipe_permohonan", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_tipe_permohonan",
                    queryPath: @"Queries\master\tipe_permohonan\master_attribute_tipe_permohonan.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_tipe_permohonan_detail", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_tipe_permohonan_detail",
                    queryPath: @"Queries\master\tipe_permohonan\master_attribute_tipe_permohonan_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_tipe_permohonan_detail_ba", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_tipe_permohonan_detail_ba",
                    queryPath: @"Queries\master\tipe_permohonan\master_attribute_tipe_permohonan_detail_ba.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_tipe_permohonan_detail_spk", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_tipe_permohonan_detail_spk",
                    queryPath: @"Queries\master\tipe_permohonan\master_attribute_tipe_permohonan_detail_spk.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });
        }

        private async Task MasterData(Settings settings)
        {
            await Utils.TrackProgress("master_attribute_flag", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionString,
                    tConnectionStr: AppSettings.ConnectionString,
                    queryPath: @"Queries\master\master_attribute_flag.sql",
                    tableName: "master_attribute_flag",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_status", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionString,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_status",
                    queryPath: @"Queries\master\master_attribute_status.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_jenis_bangunan", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_jenis_bangunan",
                    queryPath: @"Queries\master\master_attribute_jenis_bangunan.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_kepemilikan", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_kepemilikan",
                    queryPath: @"Queries\master\master_attribute_kepemilikan.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_pekerjaan", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionString,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_pekerjaan",
                    queryPath: @"Queries\master\master_attribute_pekerjaan.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_peruntukan", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_peruntukan",
                    queryPath: @"Queries\master\master_attribute_peruntukan.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_jenis_pipa", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionString,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_jenis_pipa",
                    queryPath: @"Queries\master\master_attribute_jenis_pipa.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_kwh", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionString,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_kwh",
                    queryPath: @"Queries\master\master_attribute_kwh.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_tarif_golongan", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_tarif_golongan",
                    queryPath: @"Queries\master\master_tarif_golongan.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_tarif_golongan_detail", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_tarif_golongan_detail",
                    queryPath: @"Queries\master\master_tarif_golongan_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_tarif_diameter", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_tarif_diameter",
                    queryPath: @"Queries\master\master_tarif_diameter.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_tarif_diameter_detail", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_tarif_diameter_detail",
                    queryPath: @"Queries\master\master_tarif_diameter_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_wilayah", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringBsbs,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_wilayah",
                    queryPath: @"Queries\master\master_attribute_wilayah.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_area", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringBsbs,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_area",
                    queryPath: @"Queries\master\master_attribute_area.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_rayon", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringBsbs,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_rayon",
                    queryPath: @"Queries\master\master_attribute_rayon.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_blok", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringBsbs,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_blok",
                    queryPath: @"Queries\master\master_attribute_blok.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_cabang", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringBsbs,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_cabang",
                    queryPath: @"Queries\master\master_attribute_cabang.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_kecamatan", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringBsbs,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_kecamatan",
                    queryPath: @"Queries\master\master_attribute_kecamatan.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_kelurahan", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringBsbs,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_kelurahan",
                    queryPath: @"Queries\master\master_attribute_kelurahan.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_dma", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionString,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_dma",
                    queryPath: @"Queries\master\master_attribute_dma.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_dmz", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionString,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_dmz",
                    queryPath: @"Queries\master\master_attribute_dmz.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_tarif_administrasi_lain", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringBsbs,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_tarif_administrasi_lain",
                    queryPath: @"Queries\master\master_tarif_administrasi_lain.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_tarif_pemeliharaan_lain", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringBsbs,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_tarif_pemeliharaan_lain",
                    queryPath: @"Queries\master\master_tarif_pemeliharaan_lain.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_tarif_retribusi_lain", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringBsbs,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_tarif_retribusi_lain",
                    queryPath: @"Queries\master\master_tarif_retribusi_lain.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_kolektif", async () =>
            {
                await Utils.Client(async (conn, trans) =>
                {
                    await conn.ExecuteAsync(@"
                    ALTER TABLE master_attribute_kolektif
                    CHANGE kodekolektif kodekolektif VARCHAR (50) CHARSET latin1 COLLATE latin1_swedish_ci NOT NULL", transaction: trans);
                });

                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_kolektif",
                    queryPath: @"Queries\master\master_attribute_kolektif.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_sumber_air", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_sumber_air",
                    queryPath: @"Queries\master\master_attribute_sumber_air.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_merek_meter", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_merek_meter",
                    queryPath: @"Queries\master\master_attribute_merek_meter.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_kondisi_meter", async () =>
            {
                await Utils.Client(async (conn, trans) =>
                {
                    await conn.ExecuteAsync(@"
                    ALTER TABLE master_attribute_kondisi_meter
                    CHANGE kodekondisimeter kodekondisimeter VARCHAR (50) CHARSET latin1 COLLATE latin1_swedish_ci NOT NULL", transaction: trans);
                });

                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_kondisi_meter",
                    queryPath: @"Queries\master\master_attribute_kondisi_meter.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_kelainan", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringBacameter,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_kelainan",
                    queryPath: @"Queries\master\master_attribute_kelainan.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_petugas_baca", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringBacameter,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_petugas_baca",
                    queryPath: @"Queries\master\master_attribute_petugas_baca.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_periode", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringBsbs,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_periode",
                    queryPath: @"Queries\master\master_periode.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_periode_billing", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringBsbs,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_periode_billing",
                    queryPath: @"Queries\master\master_periode_billing.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_jadwal_baca", async () =>
            {
                await Utils.ClientBacameter(async (conn, trans) =>
                {
                    var jadwalbaca = await conn.QueryAsync(@"
                                                SELECT
                                                b.nama AS petugasbaca,
                                                c.koderayon
                                                FROM
                                                jadwalbaca a
                                                JOIN petugasbaca b ON a.idpetugas=b.idpetugas
                                                JOIN rayon c ON a.idrayon=c.idrayon", transaction: trans);
                    if (jadwalbaca.Any())
                    {
                        await Utils.Client(async (conn, trans) =>
                        {
                            List<dynamic> data = [];
                            var listPetugas = await conn.QueryAsync(@"SELECT idpetugasbaca,petugasbaca FROM master_attribute_petugas_baca WHERE idpdam=@idpdam",
                                new { idpdam = settings.IdPdam }, trans);
                            var listRayon = await conn.QueryAsync(@"SELECT idrayon,koderayon FROM master_attribute_rayon WHERE idpdam=@idpdam",
                                new { idpdam = settings.IdPdam }, trans);

                            int id = 1;
                            foreach (var item in jadwalbaca)
                            {
                                dynamic o = new
                                {
                                    idpdam = settings.IdPdam,
                                    idjadwalbaca = id++,
                                    idpetugasbaca =
                                        listPetugas
                                            .Where(s => s.petugasbaca.ToLower() == item.petugasbaca.ToLower())
                                            .Select(s => s.idpetugasbaca).FirstOrDefault(),
                                    idrayon =
                                        listRayon
                                            .Where(s => s.koderayon == item.koderayon)
                                            .Select(s => s.idrayon).FirstOrDefault()
                                };

                                if (o.idpetugasbaca != null && o.idrayon != null)
                                {
                                    data.Add(o);
                                }
                            }

                            if (data.Count != 0)
                            {
                                await conn.ExecuteAsync(@"
                                                            REPLACE master_attribute_jadwal_baca (idpdam,idjadwalbaca,idpetugasbaca,idrayon)
                                                            VALUES (@idpdam,@idjadwalbaca,@idpetugasbaca,@idrayon)", data, trans);
                            }
                        });
                    }
                });
            });

            await Utils.TrackProgress("master_attribute_loket", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_loket",
                    queryPath: @"Queries\master\master_attribute_loket.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_user", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_user",
                    queryPath: @"Queries\master\master_user.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    },
                    placeholders: new()
                    {
                        { "[bacameter]", AppSettings.DatabaseBacameter },
                        { "[bsbs]", AppSettings.DatabaseBsbs },
                        { "[loket]", AppSettings.DatabaseLoket },
                    });
            });

            await Utils.TrackProgress("master_query_global", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_query_global",
                    queryPath: @"Queries\master\master_query_global.sql");
            });

            await Utils.TrackProgress("master_attribute_tipe_permohonan_config_list_data", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_tipe_permohonan_config_list_data",
                    queryPath: @"Queries\master\master_attribute_tipe_permohonan_config_list_data.sql");
            });

            await Utils.TrackProgress("master_attribute_tipe_pendaftaran_sambungan", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_tipe_pendaftaran_sambungan",
                    queryPath: @"Queries\master\master_attribute_tipe_pendaftaran_sambungan.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_sumber_pengaduan", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringLoket,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_sumber_pengaduan",
                    queryPath: @"Queries\master\master_attribute_sumber_pengaduan.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });
        }

        private async Task JenisNonair(Settings settings)
        {
            await Utils.TrackProgress("master_attribute_jenis_nonair", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_jenis_nonair",
                    queryPath: @"Queries\master\jenis_nonair\master_attribute_jenis_nonair.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_jenis_nonair_detail", async () =>
            {
                await Utils.BulkCopy(
                    sConnectionStr: AppSettings.ConnectionStringStaging,
                    tConnectionStr: AppSettings.ConnectionString,
                    tableName: "master_attribute_jenis_nonair_detail",
                    queryPath: @"Queries\master\jenis_nonair\master_attribute_jenis_nonair_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });
        }

        private async Task RotasimeterNonrutin(Settings settings)
        {
            var lastId = 0;
            var rabdetail = 0;
            dynamic? rotasimeter = null;
            await Utils.Client(async (conn, trans) =>
            {
                lastId = await conn.QueryFirstOrDefaultAsync<int>(@"SELECT IFNULL(MAX(idpermohonan),0) FROM permohonan_pelanggan_air", transaction: trans);
                rabdetail = await conn.QueryFirstOrDefaultAsync<int>(@"SELECT IFNULL(MAX(id),0) FROM permohonan_pelanggan_air_rab_detail", transaction: trans);
                rotasimeter = await conn.QueryFirstOrDefaultAsync($@"SELECT idtipepermohonan,idjenisnonair FROM master_attribute_tipe_permohonan WHERE idpdam={settings.IdPdam} AND kodetipepermohonan='GANTI_METER_NON_RUTIN'", transaction: trans);
                await conn.ExecuteAsync(
                    sql: @"
                    DELETE FROM permohonan_pelanggan_air_spk WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_rab WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_rab_detail WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_spk_pasang WHERE idpdam=@idpdam AND `idpermohonan`
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_ba WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan;",
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        idtipepermohonan = rotasimeter?.idtipepermohonan
                    },
                    transaction: trans);
            });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"
                    DROP TABLE IF EXISTS __tmp_rotasimeter;
                    CREATE TABLE __tmp_rotasimeter AS
                    SELECT
                    @id := @id+1 AS idpermohonan,
                    p.nosamb,
                    p.periode
                    FROM `rotasimeter_nonrutin` p
                    ,(SELECT @id := @lastid) AS id",
                    param: new { lastid = lastId },
                    transaction: trans);
            });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air",
                queryPath: @"Queries\rotasimeter_nonrutin\rotasimeter_nonrutin.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                    { "@tipepermohonan", rotasimeter.idtipepermohonan },
                },
                placeholders: new()
                {
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_spk",
                queryPath: @"Queries\rotasimeter_nonrutin\spk_rotasimeter_nonrutin.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_rab",
                queryPath: @"Queries\rotasimeter_nonrutin\rab_rotasimeter_nonrutin.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@jenisnonair", rotasimeter.idjenisnonair },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_rab_detail",
                queryPath: @"Queries\rotasimeter_nonrutin\rabdetail_rotasimeter_nonrutin.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", rabdetail },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_spk_pasang",
                queryPath: @"Queries\rotasimeter_nonrutin\spkp_rotasimeter_nonrutin.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\rotasimeter_nonrutin\ba_rotasimeter_nonrutin.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"
                    DROP TABLE IF EXISTS __tmp_rotasimeter",
                    transaction: trans);
            });
        }

        private async Task Rotasimeter(Settings settings)
        {
            var lastId = 0;
            var rotasimeter = 0;
            await Utils.Client(async (conn, trans) =>
            {
                lastId = await conn.QueryFirstOrDefaultAsync<int>(@"SELECT IFNULL(MAX(idpermohonan),0) FROM permohonan_pelanggan_air", transaction: trans);
                rotasimeter = await conn.QueryFirstOrDefaultAsync<int>($@"SELECT idtipepermohonan FROM master_attribute_tipe_permohonan WHERE idpdam={settings.IdPdam} AND kodetipepermohonan='GANTI_METER_RUTIN'", transaction: trans);
                await conn.ExecuteAsync(
                    sql: @"
                    DELETE FROM permohonan_pelanggan_air_spk WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_spk_pasang WHERE idpdam=@idpdam AND `idpermohonan`
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_ba WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan;",
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        idtipepermohonan = rotasimeter
                    },
                    transaction: trans);
            });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"
                    DROP TABLE IF EXISTS __tmp_rotasimeter;
                    CREATE TABLE __tmp_rotasimeter AS
                    SELECT
                    @id := @id+1 AS idpermohonan,
                    p.nosamb,
                    p.periode
                    FROM `rotasimeter` p
                    ,(SELECT @id := @lastid) AS id",
                    param: new { lastid = lastId },
                    transaction: trans);
            });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air",
                queryPath: @"Queries\rotasimeter\rotasimeter.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                    { "@tipepermohonan", rotasimeter },
                },
                placeholders: new()
                {
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_spk",
                queryPath: @"Queries\rotasimeter\spk_rotasimeter.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_spk_pasang",
                queryPath: @"Queries\rotasimeter\spkp_rotasimeter.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\rotasimeter\ba_rotasimeter.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"
                    DROP TABLE IF EXISTS __tmp_rotasimeter",
                    transaction: trans);
            });
        }

        private async Task KoreksiRekair(Settings settings)
        {
            var lastId = 0;
            var krekair = 0;
            await Utils.Client(async (conn, trans) =>
            {
                lastId = await conn.QueryFirstOrDefaultAsync<int>(@"SELECT IFNULL(MAX(idpermohonan),0) FROM permohonan_pelanggan_air", transaction: trans);
                krekair = await conn.QueryFirstOrDefaultAsync<int>($@"SELECT idtipepermohonan FROM master_attribute_tipe_permohonan WHERE idpdam={settings.IdPdam} AND kodetipepermohonan='KREKAIR'", transaction: trans);
                await conn.ExecuteAsync(
                    sql: @"
                    DELETE FROM `permohonan_pelanggan_air_koreksi_rekening` WHERE idpdam=@idpdam AND `idpermohonan` 
                    IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan;",
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        idtipepermohonan = krekair
                    },
                    transaction: trans);
            });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"
                    DROP TABLE IF EXISTS __tmp_koreksi_rek;
                    CREATE TABLE __tmp_koreksi_rek AS
                    SELECT 
                    @id := @id+1 AS idpermohonan,
                    a.nomor,
                    CAST(CONCAT(a.status,'[',COUNT(*),']') AS CHAR) AS `status`
                    FROM (
                    SELECT
                    p.`nomor`,
                    IF(d.`id` IS NULL,
                     'Menunggu Usulan Koreksi',
                     '(Selesai) Sudah Verifikasi Pusat') AS `status`
                    FROM `permohonan_koreksi_rek` p
                    LEFT JOIN `ba_usulan_koreksi_rekening_periode` d ON d.`nomorpermohonan`=p.`nomor`
                    WHERE p.`flaghapus`=0 ) a
                    ,(SELECT @id := @lastid) AS id
                    GROUP BY a.nomor,a.status",
                    param: new
                    {
                        lastid = lastId
                    },
                    transaction: trans);
            });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air",
                queryPath: @"Queries\koreksi_rekair\koreksi_rekair.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@tipepermohonan", krekair },
                },
                placeholders: new()
                {
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_koreksi_rekening",
                queryPath: @"Queries\koreksi_rekair\koreksi_rekair_periode.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"DROP TABLE IF EXISTS __tmp_koreksi_rek",
                    transaction: trans);
            });
        }

        private async Task SambungBaru(Settings settings)
        {
            var lastId = 0;
            var rabDetail = 0;
            var sambBaru = 0;
            await Utils.Client(async (conn, trans) =>
            {
                lastId = await conn.QueryFirstOrDefaultAsync<int>(@"SELECT IFNULL(MAX(idpermohonan),0) FROM permohonan_non_pelanggan", transaction: trans);
                rabDetail = await conn.QueryFirstOrDefaultAsync<int>(@"SELECT IFNULL(MAX(id),0) FROM permohonan_non_pelanggan_rab_detail", transaction: trans);
                sambBaru = await conn.QueryFirstOrDefaultAsync<int>($@"SELECT idtipepermohonan FROM master_attribute_tipe_permohonan WHERE idpdam={settings.IdPdam} AND kodetipepermohonan='SAMBUNGAN_BARU_AIR'", transaction: trans);
                await conn.ExecuteAsync(
                    sql: @"
                    DELETE FROM `permohonan_non_pelanggan_spk` WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_non_pelanggan_rab WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_non_pelanggan_rab_detail WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_non_pelanggan_spk_pasang WHERE idpdam=@idpdam AND `idpermohonan`
                     IN (SELECT `idpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_non_pelanggan_ba WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan;",
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        idtipepermohonan = sambBaru
                    },
                    transaction: trans);
            });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"
                    DROP TABLE IF EXISTS __tmp_sambung_baru;
                    CREATE TABLE __tmp_sambung_baru AS
                    SELECT
                    @id := @id+1 AS idpermohonan,
                    p.nomorreg
                    FROM `pendaftaran` p
                    ,(SELECT @id := @lastid) AS id
                    WHERE p.flaghapus = 0",
                    param: new
                    {
                        lastid = lastId
                    },
                    transaction: trans);
            });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_non_pelanggan",
                queryPath: @"Queries\sambung_baru\sambung_baru.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                    { "@tipepermohonan", sambBaru },
                },
                placeholders: new()
                {
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_non_pelanggan_spk",
                queryPath: @"Queries\sambung_baru\spk_sambung_baru.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_non_pelanggan_rab",
                queryPath: @"Queries\sambung_baru\rab_sambung_baru.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_non_pelanggan_rab_detail",
                queryPath: @"Queries\sambung_baru\rabdetail_sambung_baru.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", rabDetail },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_non_pelanggan_spk_pasang",
                queryPath: @"Queries\sambung_baru\spkp_sambung_baru.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_non_pelanggan_ba",
                queryPath: @"Queries\sambung_baru\ba_sambung_baru.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"DROP TABLE IF EXISTS __tmp_sambung_baru",
                    transaction: trans);
            });
        }

        private async Task BukaSegel(Settings settings)
        {
            var lastId = 0;
            var bukaSegel = 0;
            await Utils.Client(async (conn, trans) =>
            {
                lastId = await conn.QueryFirstOrDefaultAsync<int>(@"SELECT IFNULL(MAX(idpermohonan),0) FROM permohonan_pelanggan_air", transaction: trans);
                bukaSegel = await conn.QueryFirstOrDefaultAsync<int>($@"SELECT idtipepermohonan FROM master_attribute_tipe_permohonan WHERE idpdam={settings.IdPdam} AND kodetipepermohonan='BUKA_SEGEL'", transaction: trans);
                await conn.ExecuteAsync(
                    sql: @"
                    DELETE FROM permohonan_pelanggan_air_spk_pasang WHERE idpdam=@idpdam AND `idpermohonan`
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_ba WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan;",
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        idtipepermohonan = bukaSegel
                    },
                    transaction: trans);
            });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: $@"
                    DROP TABLE IF EXISTS __tmp_buka_segel;
                    CREATE TABLE __tmp_buka_segel AS
                    SELECT
                    @id := @id+1 AS idpermohonan,
                    per.nomor
                    FROM `permohonan_bukasegel` per
                    JOIN pelanggan pel ON pel.nosamb = per.nosamb
                    ,(SELECT @id := @lastid) AS id
                    WHERE per.flaghapus = 0",
                    param: new { lastid = lastId },
                    transaction: trans);
            });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air",
                queryPath: @"Queries\buka_segel\buka_segel.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                    { "@tipepermohonan", bukaSegel },
                },
                placeholders: new()
                {
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_spk_pasang",
                queryPath: @"Queries\buka_segel\spkp_buka_segel.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\buka_segel\ba_buka_segel.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"DROP TABLE IF EXISTS __tmp_buka_segel",
                    transaction: trans);
            });
        }

        private async Task SambungKembali(Settings settings)
        {
            var lastId = 0;
            var rabdetail = 0;
            dynamic? sambKembali = null;
            await Utils.Client(async (conn, trans) =>
            {
                lastId = await conn.QueryFirstOrDefaultAsync<int>(@"SELECT IFNULL(MAX(idpermohonan),0) FROM permohonan_pelanggan_air", transaction: trans);
                rabdetail = await conn.QueryFirstOrDefaultAsync<int>(@"SELECT IFNULL(MAX(id),0) FROM permohonan_pelanggan_air_rab_detail", transaction: trans);
                sambKembali = await conn.QueryFirstOrDefaultAsync($@"SELECT idtipepermohonan,idjenisnonair FROM master_attribute_tipe_permohonan WHERE idpdam={settings.IdPdam} AND kodetipepermohonan='SAMBUNG_KEMBALI'", transaction: trans);
                await conn.ExecuteAsync(
                    sql: @"
                    DELETE FROM `permohonan_pelanggan_air_spk` WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_rab WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_rab_detail WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_spk_pasang WHERE idpdam=@idpdam AND `idpermohonan`
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_ba WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan;",
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        idtipepermohonan = sambKembali?.idtipepermohonan
                    },
                    transaction: trans);
            });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: $@"
                    DROP TABLE IF EXISTS __tmp_sambung_kembali;
                    CREATE TABLE __tmp_sambung_kembali AS
                    SELECT
                    @id := @id+1 AS idpermohonan,
                    per.nomor
                    FROM permohonan_sambung_kembali per
                    JOIN pelanggan pel ON pel.nosamb = per.nosamb
                    ,(SELECT @id := @lastid) AS id
                    WHERE per.flaghapus = 0;",
                    param: new
                    {
                        lastid = lastId
                    },
                    transaction: trans);
            });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air",
                queryPath: @"Queries\sambung_kembali\sambung_kembali.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                    { "@tipepermohonan", sambKembali.idtipepermohonan },
                },
                placeholders: new()
                {
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_spk",
                queryPath: @"Queries\sambung_kembali\spk_sambung_kembali.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_rab",
                queryPath: @"Queries\sambung_kembali\rab_sambung_kembali.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@jenisnonair", sambKembali.idjenisnonair },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_rab_detail",
                queryPath: @"Queries\sambung_kembali\rabdetail_sambung_kembali.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", rabdetail },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_spk_pasang",
                queryPath: @"Queries\sambung_kembali\spkp_sambung_kembali.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\sambung_kembali\ba_sambung_kembali.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"DROP TABLE IF EXISTS __tmp_sambung_kembali",
                    transaction: trans);
            });
        }

        private async Task RubahRayon(Settings settings)
        {
            var lastId = 0;
            dynamic? rubahRayon = null;
            await Utils.Client(async (conn, trans) =>
            {
                lastId = await conn.QueryFirstOrDefaultAsync<int>(@"SELECT IFNULL(MAX(idpermohonan),0) FROM permohonan_pelanggan_air", transaction: trans);
                rubahRayon = await conn.QueryFirstOrDefaultAsync($@"SELECT idtipepermohonan,idjenisnonair FROM master_attribute_tipe_permohonan WHERE idpdam={settings.IdPdam} AND kodetipepermohonan='RUBAH_RAYON'", transaction: trans);
                
                await conn.ExecuteAsync(
                    sql: @"
                    DELETE FROM permohonan_pelanggan_air_rab WHERE idpdam=@idpdam AND `idpermohonan`
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_spk_pasang WHERE idpdam=@idpdam AND `idpermohonan`
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_ba WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan;",
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        idtipepermohonan = rubahRayon?.idtipepermohonan
                    },
                    transaction: trans);
            });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"
                    DROP TABLE IF EXISTS __tmp_permohonan_rubah_rayon;
                    CREATE TABLE __tmp_permohonan_rubah_rayon AS
                    SELECT
                    @id := @id+1 AS idpermohonan,
                    per.nomor
                    FROM permohonan_rubah_rayon per
                    JOIN pelanggan pel ON pel.nosamb = per.nosamb
                    ,(SELECT @id := @lastid) AS id
                    WHERE per.flaghapus = 0",
                    param: new
                    {
                        lastid = lastId,
                    },
                    transaction: trans);
            });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air",
                queryPath: @"Queries\rubah_rayon\rubah_rayon.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                    { "@tipepermohonan", rubahRayon.idtipepermohonan },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_rab",
                queryPath: @"Queries\rubah_rayon\rab_rubah_rayon.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@jenisnonair", rubahRayon.idjenisnonair },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_spk_pasang",
                queryPath: @"Queries\rubah_rayon\spkp_rubah_rayon.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\rubah_rayon\ba_rubah_rayon.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"DROP TABLE IF EXISTS __tmp_permohonan_rubah_rayon;",
                    transaction: trans);
            });
        }

        private async Task RubahGolongan(Settings settings)
        {
            var lastId = 0;
            var rubahGol = 0;
            await Utils.Client(async (conn, trans) =>
            {
                lastId = await conn.QueryFirstOrDefaultAsync<int>(@"SELECT IFNULL(MAX(idpermohonan),0) FROM permohonan_pelanggan_air", transaction: trans);
                rubahGol = await conn.QueryFirstOrDefaultAsync<int>($@"SELECT idtipepermohonan FROM master_attribute_tipe_permohonan WHERE idpdam={settings.IdPdam} AND kodetipepermohonan='RUBAH_GOL'", transaction: trans);
                await conn.ExecuteAsync(
                    sql: @"
                    DELETE FROM permohonan_pelanggan_air_spk WHERE idpdam=@idpdam AND `idpermohonan`
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_ba WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan;",
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        idtipepermohonan = rubahGol
                    },
                    transaction: trans);
            });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air",
                queryPath: @"Queries\rubah_gol\rubah_gol.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                    { "@tipepermohonan", rubahGol },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: $@"
                    DROP TEMPORARY TABLE IF EXISTS __tmp_userloket;
                    CREATE TEMPORARY TABLE __tmp_userloket AS
                    SELECT
                    @idpdam,
                    @id := @id + 1 AS iduser,
                    a.nama,
                    a.namauser
                    FROM (
                    SELECT nama,namauser,`passworduser`,alamat,aktif FROM {AppSettings.DatabaseBacameter}.`userakses`
                    UNION
                    SELECT nama,namauser,`passworduser`,NULL AS alamat,aktif FROM {AppSettings.DatabaseBsbs}.`userakses`
                    UNION
                    SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM `userloket`
                    UNION
                    SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM `userbshl`
                    ) a,
                    (SELECT @id := 0) AS id
                    GROUP BY a.namauser;

                    DROP TABLE IF EXISTS __tmp_permohonan_rubah_gol;
                    CREATE TABLE __tmp_permohonan_rubah_gol AS
                    SELECT
                    @id := @id+1 AS id,
                    rg.nomor,
                    usr.iduser
                    FROM permohonan_rubah_gol rg
                    JOIN pelanggan pel ON pel.nosamb = rg.nosamb
                    LEFT JOIN __tmp_userloket usr ON usr.nama = SUBSTRING_INDEX(rg.urutannonair,'.RUBAH_GOL.',1)
                    ,(SELECT @id := @lastid) AS id
                    WHERE rg.flaghapus = 0",
                    param: new
                    {
                        lastid = lastId
                    }
                    , trans);
            });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_spk",
                queryPath: @"Queries\rubah_gol\spk_rubah_gol.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\rubah_gol\ba_rubah_gol.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(@"DROP TABLE IF EXISTS __tmp_permohonan_rubah_gol", transaction: trans);
            });
        }

        private async Task BalikNama(Settings settings)
        {
            var lastId = 0;
            var idBalikNama = 0;
            await Utils.Client(async (conn, trans) =>
            {
                lastId = await conn.QueryFirstOrDefaultAsync<int>(@"SELECT IFNULL(MAX(idpermohonan),0) FROM permohonan_pelanggan_air", transaction: trans);
                idBalikNama = await conn.QueryFirstOrDefaultAsync<int>($@"SELECT idtipepermohonan FROM master_attribute_tipe_permohonan WHERE idpdam={settings.IdPdam} AND kodetipepermohonan='BALIK_NAMA'", transaction: trans);
                
                await conn.ExecuteAsync(
                    sql: @"
                    DELETE FROM permohonan_pelanggan_air_ba WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan;",
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        idtipepermohonan = idBalikNama
                    },
                    transaction: trans);
            });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air",
                queryPath: @"Queries\balik_nama\balik_nama.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                    { "@tipepermohonan", idBalikNama },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync($@"
                DROP TEMPORARY TABLE IF EXISTS __tmp_userloket;
                CREATE TEMPORARY TABLE __tmp_userloket AS
                SELECT
                @idpdam,
                @id := @id + 1 AS iduser,
                a.nama,
                a.namauser
                FROM (
                SELECT nama,namauser,`passworduser`,alamat,aktif FROM {AppSettings.DatabaseBacameter}.`userakses`
                UNION
                SELECT nama,namauser,`passworduser`,NULL AS alamat,aktif FROM {AppSettings.DatabaseBsbs}.`userakses`
                UNION
                SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM `userloket`
                UNION
                SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM `userbshl`
                ) a,
                (SELECT @id := 0) AS id
                GROUP BY a.namauser;

                CREATE TABLE __tmp_permohonan_balik_nama AS
                SELECT
                @id := @id+1 AS id,
                bn.nomor,
                usr.iduser 
                FROM permohonan_balik_nama bn
                JOIN pelanggan pel ON pel.nosamb = bn.nosamb
                LEFT JOIN __tmp_userloket usr ON usr.nama = SUBSTRING_INDEX(bn.urutannonair,'.BALIK NAMA.',1)
                ,(SELECT @id := @lastid) AS id
                WHERE bn.flaghapus = 0", new { lastid = lastId }, trans);
            });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\balik_nama\ba_balik_nama.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(@"DROP TABLE IF EXISTS __tmp_permohonan_balik_nama", transaction: trans);
            });
        }

        private async Task Pengaduan(Settings settings)
        {
            IEnumerable<dynamic>? tipe = [];

            await Utils.Client(async (conn, trans) =>
            {
                tipe = await conn.QueryAsync(@"
                SELECT a.idtipepermohonan,a.idjenisnonair,b.kodejenisnonair
                FROM master_attribute_tipe_permohonan a
                JOIN master_attribute_jenis_nonair b ON b.idpdam = a.idpdam AND b.idjenisnonair = a.idjenisnonair
                WHERE a.idpdam = @idpdam AND a.flaghapus = 0 AND a.kategori = 'Pengaduan'", new { idpdam = settings.IdPdam }, trans);

                if (tipe != null)
                {
                    await conn.ExecuteAsync(
                        sql: @"
                        DELETE FROM permohonan_pelanggan_air_spk WHERE idpdam=@idpdam AND `idpermohonan`
                         IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan` in @tipepermohonan);

                        DELETE FROM permohonan_pelanggan_air_ba WHERE idpdam=@idpdam AND `idpermohonan` 
                         IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan` in @tipepermohonan);

                        DELETE FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan` in @tipepermohonan;",
                        param: new
                        {
                            idpdam = settings.IdPdam,
                            tipepermohonan = tipe.Select(s => s.idtipepermohonan).ToList()
                        },
                        transaction: trans);
                }
            });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                if (tipe != null)
                {
                    await conn.ExecuteAsync(@"
                    DROP TABLE IF EXISTS __tmp_tipepermohonan;

                    CREATE TABLE __tmp_tipepermohonan (
                    idtipepermohonan INT,
                    idjenisnonair INT,
                    kodejenisnonair VARCHAR(50)
                    )", transaction: trans);

                    await conn.ExecuteAsync(@"
                    INSERT INTO __tmp_tipepermohonan
                    VALUES (@idtipepermohonan,@idjenisnonair,@kodejenisnonair)", tipe, trans);
                }
            });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air",
                queryPath: @"Queries\pengaduan\pengaduan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_spk",
                queryPath: @"Queries\pengaduan\spk_pengaduan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                });

            await Utils.BulkCopy(
                sConnectionStr: AppSettings.ConnectionStringLoket,
                tConnectionStr: AppSettings.ConnectionString,
                tableName: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\pengaduan\ba_pengaduan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                });

            await Utils.ClientLoket(async (conn, trans) =>
            {
                await conn.ExecuteAsync(@"DROP TABLE IF EXISTS __tmp_tipepermohonan", transaction: trans);
            });
        }
    }
}
