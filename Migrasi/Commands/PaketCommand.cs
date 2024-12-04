using Dapper;
using Migrasi.Helpers;
using Spectre.Console;
using Spectre.Console.Cli;
using System.Diagnostics;

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
                            .AddRow("Periode H-4", periodeHMin4.ToString()!)
                            .AddRow("DB Bacameter V4", AppSettings.DBNameBacameter)
                            .AddRow("DB Billing V4", AppSettings.DBNameBilling)
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
                                    await Utils.TrackProgress("cleanup data pelanggan", async () =>
                                    {
                                        ctx.Status("Tambah primary key id pelanggan");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var cek = await conn.QueryFirstOrDefaultAsync<int?>("SELECT 1 FROM information_schema.COLUMNS WHERE table_schema=@schema AND table_name='pelanggan' AND column_name='id'",
                                                new { schema = AppSettings.DBNameBilling }, trans);
                                            if (cek is null)
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\Patches\tambah_field_id_tabel_pelanggan.sql");
                                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                            }
                                        });

                                        ctx.Status("cek golongan");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_golongan.sql");
                                            query = query.Replace("[table]", "pelanggan");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek diameter");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_diameter.sql");
                                            query = query.Replace("[table]", "pelanggan");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek merek meter");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_merek_meter.sql");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek kelurahan");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kelurahan.sql");
                                            query = query.Replace("[table]", "pelanggan");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek kolektif");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kolektif.sql");
                                            query = query.Replace("[table]", "pelanggan");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek sumber air");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_sumber_air.sql");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek blok");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_blok.sql");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek kondisi meter");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kondisi_meter.sql");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek administrasi lain");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_adm_lain.sql");
                                            query = query.Replace("[table]", "pelanggan");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek pemeliharaan lain");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_pem_lain.sql");
                                            query = query.Replace("[table]", "pelanggan");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek retribusi lain");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_ret_lain.sql");
                                            query = query.Replace("[table]", "pelanggan");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });
                                    });

                                    await Utils.TrackProgress("data master", async () =>
                                    {
                                        ctx.Status("proses flag");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            queryPath: @"Queries\master_attribute_flag.sql",
                                            tableName: "master_attribute_flag",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses status");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_status",
                                            queryPath: @"Queries\master_attribute_status.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses jenis bangunan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_jenis_bangunan",
                                            queryPath: @"Queries\master_attribute_jenis_bangunan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses kepemilikan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kepemilikan",
                                            queryPath: @"Queries\master_attribute_kepemilikan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses pekerjaan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_pekerjaan",
                                            queryPath: @"Queries\master_attribute_pekerjaan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses peruntukan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_peruntukan",
                                            queryPath: @"Queries\master_attribute_peruntukan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses jenis pipa");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_jenis_pipa",
                                            queryPath: @"Queries\master_attribute_jenis_pipa.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses kwh");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kwh",
                                            queryPath: @"Queries\master_attribute_kwh.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses golongan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_golongan",
                                            queryPath: @"Queries\master_tarif_golongan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_golongan_detail",
                                            queryPath: @"Queries\master_tarif_golongan_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses diameter");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_diameter",
                                            queryPath: @"Queries\master_tarif_diameter.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_diameter_detail",
                                            queryPath: @"Queries\master_tarif_diameter_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses wilayah");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_wilayah",
                                            queryPath: @"Queries\master_attribute_wilayah.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses area");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_area",
                                            queryPath: @"Queries\master_attribute_area.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses rayon");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_rayon",
                                            queryPath: @"Queries\master_attribute_rayon.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses blok");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_blok",
                                            queryPath: @"Queries\master_attribute_blok.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses cabang");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_cabang",
                                            queryPath: @"Queries\master_attribute_cabang.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses kecamatan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kecamatan",
                                            queryPath: @"Queries\master_attribute_kecamatan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses kelurahan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kelurahan",
                                            queryPath: @"Queries\master_attribute_kelurahan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses dma");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_dma",
                                            queryPath: @"Queries\master_attribute_dma.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses dmz");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_dmz",
                                            queryPath: @"Queries\master_attribute_dmz.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses administrasi lain");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_administrasi_lain",
                                            queryPath: @"Queries\master_tarif_administrasi_lain.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses pemeliharaan lain");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_pemeliharaan_lain",
                                            queryPath: @"Queries\master_tarif_pemeliharaan_lain.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses retribusi lain");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_retribusi_lain",
                                            queryPath: @"Queries\master_tarif_retribusi_lain.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses kolektif");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kolektif",
                                            queryPath: @"Queries\master_attribute_kolektif.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses sumber air");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_sumber_air",
                                            queryPath: @"Queries\master_attribute_sumber_air.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses merek meter");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_merek_meter",
                                            queryPath: @"Queries\master_attribute_merek_meter.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses kondisi meter");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kondisi_meter",
                                            queryPath: @"Queries\master_attribute_kondisi_meter.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses kelainan");
                                        await Utils.Client(async (conn, trans) =>
                                        {
                                            await conn.ExecuteAsync($"DELETE FROM master_attribute_kelainan WHERE idpdam={settings.IdPdam}",
                                                 transaction: trans);
                                        });
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var cek = await conn.QueryAsync("SELECT column_name FROM information_schema.COLUMNS WHERE table_schema=@schema AND table_name='kelainan'",
                                                new { schema = AppSettings.DBNameBilling }, trans);
                                            if (cek.Any())
                                            {
                                                if (!cek.Where(s => s.column_name == "idkelainan").Any())
                                                {
                                                    await conn.ExecuteAsync("ALTER TABLE kelainan CHANGE COLUMN id idkelainan INT(11)", transaction: trans);
                                                }

                                                if (!cek.Where(s => s.column_name == "kodekelainan").Any())
                                                {
                                                    await conn.ExecuteAsync("ALTER TABLE kelainan ADD COLUMN kodekelainan VARCHAR(10)", transaction: trans);
                                                }

                                                if (!cek.Where(s => s.column_name == "idx").Any())
                                                {
                                                    await conn.ExecuteAsync("ALTER TABLE kelainan ADD COLUMN idx int(11)", transaction: trans);
                                                }

                                                if (!cek.Where(s => s.column_name == "aktif").Any())
                                                {
                                                    await conn.ExecuteAsync("ALTER TABLE kelainan ADD COLUMN aktif varchar(1)", transaction: trans);
                                                }
                                            }
                                        });
                                        await Utils.ClientBacameter(async (conn, trans) =>
                                        {
                                            var kelainan = await conn.QueryAsync(@"SELECT idkelainan,kodekelainan,kelainan,idx,aktif FROM kelainan", transaction: trans);
                                            if (kelainan.Any())
                                            {
                                                await Utils.ClientBilling(async (conn, trans) =>
                                                {
                                                    await conn.ExecuteAsync("DELETE FROM kelainan", transaction: trans);
                                                    await conn.ExecuteAsync("REPLACE INTO kelainan (idkelainan,kodekelainan,kelainan,idx,aktif) VALUES (@idkelainan,@kodekelainan,@kelainan,@idx,@aktif)",
                                                        kelainan, trans);
                                                });
                                            }
                                        });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBacameter,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kelainan",
                                            queryPath: @"Queries\master_attribute_kelainan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses petugas baca");
                                        await Utils.Client(async (conn, trans) =>
                                        {
                                            await conn.ExecuteAsync($"DELETE FROM master_attribute_petugas_baca WHERE idpdam={settings.IdPdam}",
                                                 transaction: trans);
                                        });
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var cek = await conn.QueryAsync("SELECT column_name FROM information_schema.COLUMNS WHERE table_schema=@schema AND table_name='pembacameter'",
                                                new { schema = AppSettings.DBNameBilling }, trans);
                                            if (cek.Any())
                                            {
                                                if (!cek.Where(s => s.column_name == "idpetugas").Any())
                                                {
                                                    await conn.ExecuteAsync("ALTER TABLE pembacameter ADD COLUMN idpetugas int(11)", transaction: trans);
                                                }

                                                if (!cek.Where(s => s.column_name == "kodepetugas").Any())
                                                {
                                                    await conn.ExecuteAsync("ALTER TABLE pembacameter ADD COLUMN kodepetugas varchar(5)", transaction: trans);
                                                }

                                                if (!cek.Where(s => s.column_name == "aktif").Any())
                                                {
                                                    await conn.ExecuteAsync("ALTER TABLE pembacameter ADD COLUMN aktif varchar(1)", transaction: trans);
                                                }
                                            }
                                        });
                                        await Utils.ClientBacameter(async (conn, trans) =>
                                        {
                                            var petugas = await conn.QueryAsync(@"SELECT idpetugas,kodepetugas,nama,aktif FROM petugasbaca", transaction: trans);
                                            if (petugas.Any())
                                            {
                                                await Utils.ClientBilling(async (conn, trans) =>
                                                {
                                                    await conn.ExecuteAsync("DELETE FROM pembacameter", transaction: trans);
                                                    await conn.ExecuteAsync("REPLACE INTO pembacameter (idpetugas,kodepetugas,nama,aktif) VALUES (@idpetugas,@kodepetugas,@nama,@aktif)",
                                                        petugas, trans);
                                                });
                                            }
                                        });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_petugas_baca",
                                            queryPath: @"Queries\master_attribute_petugas_baca.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses periode");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_periode",
                                            queryPath: @"Queries\master_periode.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_periode_billing",
                                            queryPath: @"Queries\master_periode_billing.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses jadwal baca");
                                        await Utils.ClientBacameter(async (conn, trans) =>
                                        {
                                            var jadwalbaca = await conn.QueryAsync(@"SELECT
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
                                                        await conn.ExecuteAsync(@"REPLACE master_attribute_jadwal_baca (idpdam,idjadwalbaca,idpetugasbaca,idrayon)
                                                    VALUES (@idpdam,@idjadwalbaca,@idpetugasbaca,@idrayon)", data, trans);
                                                    }
                                                });
                                            }
                                        });
                                    });

                                    await Utils.TrackProgress("pelanggan air", async () =>
                                    {
                                        ctx.Status("proses pelanggan air");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_pelanggan_air",
                                            queryPath: @"Queries\master_pelanggan_air.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        ctx.Status("proses pelanggan air detail");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_pelanggan_air_detail",
                                            queryPath: @"Queries\master_pelanggan_air_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        ctx.Status("update latlong pelanggan air");
                                        await Utils.ClientBacameter(async (conn, trans) =>
                                        {
                                            var month = (periodeHMin4 + 3).ToString().Substring(4, 2);
                                            var year = (periodeHMin4 + 3).ToString().Substring(2, 2);
                                            var pel = await conn.QueryAsync($"SELECT idpelanggan,latitude,longitude FROM hasilbaca{month}{year}", transaction: trans);
                                            if (pel.Any())
                                            {
                                                await Utils.Client(async (conn, trans) =>
                                                {
                                                    await conn.ExecuteAsync($"UPDATE master_pelanggan_air SET latitude=@latitude,longitude=@longitude WHERE idpdam={settings.IdPdam} AND nosamb=@idpelanggan", pel, trans);
                                                });
                                            }
                                        });
                                    });

                                    for (int i = periodeHMin4; i < periodeHMin4 + 4; i++)
                                    {
                                        await Utils.TrackProgress($"cleanup data drd{i}", async () =>
                                        {
                                            ctx.Status("cek golongan");
                                            await Utils.ClientBilling(async (conn, trans) =>
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_golongan.sql");
                                                query = query.Replace("[table]", $"drd{i}");
                                                await conn.ExecuteAsync(query, transaction: trans);
                                            });

                                            ctx.Status("cek diameter");
                                            await Utils.ClientBilling(async (conn, trans) =>
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_diameter.sql");
                                                query = query.Replace("[table]", $"drd{i}");
                                                await conn.ExecuteAsync(query, transaction: trans);
                                            });

                                            ctx.Status("cek kelurahan");
                                            await Utils.ClientBilling(async (conn, trans) =>
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kelurahan.sql");
                                                query = query.Replace("[table]", $"drd{i}");
                                                await conn.ExecuteAsync(query, transaction: trans);
                                            });

                                            ctx.Status("cek kolektif");
                                            await Utils.ClientBilling(async (conn, trans) =>
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kolektif.sql");
                                                query = query.Replace("[table]", $"drd{i}");
                                                await conn.ExecuteAsync(query, transaction: trans);
                                            });

                                            ctx.Status("cek administrasi lain");
                                            await Utils.ClientBilling(async (conn, trans) =>
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_adm_lain.sql");
                                                query = query.Replace("[table]", $"drd{i}");
                                                await conn.ExecuteAsync(query, transaction: trans);
                                            });

                                            ctx.Status("cek pemeliharaan lain");
                                            await Utils.ClientBilling(async (conn, trans) =>
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_pem_lain.sql");
                                                query = query.Replace("[table]", $"drd{i}");
                                                await conn.ExecuteAsync(query, transaction: trans);
                                            });

                                            ctx.Status("cek retribusi lain");
                                            await Utils.ClientBilling(async (conn, trans) =>
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_ret_lain.sql");
                                                query = query.Replace("[table]", $"drd{i}");
                                                await conn.ExecuteAsync(query, transaction: trans);
                                            });
                                        });

                                        ctx.Status($"proses drd{i}");
                                        await Utils.TrackProgress($"data drd{i}", async () =>
                                        {
                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringBilling,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air",
                                                queryPath: @"Queries\drd.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam }
                                                },
                                                placeholders: new()
                                                {
                                                    { "[tahunbulan]", i.ToString() }
                                                });
                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringBilling,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air_detail",
                                                queryPath: @"Queries\drd_detail.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam }
                                                },
                                                placeholders: new()
                                                {
                                                    { "[tahunbulan]", i.ToString() }
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
                            .AddRow("DB Bacameter V4", AppSettings.DBNameBacameter)
                            .AddRow("DB Billing V4", AppSettings.DBNameBilling)
                            .AddRow("DB Loket V4", AppSettings.DBNameLoket)
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
                                    await Utils.TrackProgress("cleanup data pelanggan", async () =>
                                    {
                                        ctx.Status("Tambah primary key id pelanggan");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var cek = await conn.QueryFirstOrDefaultAsync<int?>("SELECT 1 FROM information_schema.COLUMNS WHERE table_schema=@schema AND table_name='pelanggan' AND column_name='id'",
                                                new { schema = AppSettings.DBNameBilling }, trans);
                                            if (cek is null)
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\Patches\tambah_field_id_tabel_pelanggan.sql");
                                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                            }
                                        });

                                        ctx.Status("cek golongan");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_golongan.sql");
                                            query = query.Replace("[table]", "pelanggan");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek diameter");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_diameter.sql");
                                            query = query.Replace("[table]", "pelanggan");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek merek meter");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_merek_meter.sql");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek kelurahan");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kelurahan.sql");
                                            query = query.Replace("[table]", "pelanggan");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek kolektif");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kolektif.sql");
                                            query = query.Replace("[table]", "pelanggan");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek sumber air");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_sumber_air.sql");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek blok");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_blok.sql");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek kondisi meter");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kondisi_meter.sql");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek administrasi lain");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_adm_lain.sql");
                                            query = query.Replace("[table]", "pelanggan");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek pemeliharaan lain");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_pem_lain.sql");
                                            query = query.Replace("[table]", "pelanggan");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek retribusi lain");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_ret_lain.sql");
                                            query = query.Replace("[table]", "pelanggan");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });
                                    });

                                    await Utils.TrackProgress("data master", async () =>
                                    {
                                        ctx.Status("proses flag");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            queryPath: @"Queries\master_attribute_flag.sql",
                                            tableName: "master_attribute_flag",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses status");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_status",
                                            queryPath: @"Queries\master_attribute_status.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses jenis bangunan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_jenis_bangunan",
                                            queryPath: @"Queries\master_attribute_jenis_bangunan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses kepemilikan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kepemilikan",
                                            queryPath: @"Queries\master_attribute_kepemilikan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses pekerjaan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_pekerjaan",
                                            queryPath: @"Queries\master_attribute_pekerjaan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses peruntukan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_peruntukan",
                                            queryPath: @"Queries\master_attribute_peruntukan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses jenis pipa");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_jenis_pipa",
                                            queryPath: @"Queries\master_attribute_jenis_pipa.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses kwh");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kwh",
                                            queryPath: @"Queries\master_attribute_kwh.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses golongan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_golongan",
                                            queryPath: @"Queries\master_tarif_golongan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_golongan_detail",
                                            queryPath: @"Queries\master_tarif_golongan_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses diameter");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_diameter",
                                            queryPath: @"Queries\master_tarif_diameter.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_diameter_detail",
                                            queryPath: @"Queries\master_tarif_diameter_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses wilayah");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_wilayah",
                                            queryPath: @"Queries\master_attribute_wilayah.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses area");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_area",
                                            queryPath: @"Queries\master_attribute_area.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses rayon");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_rayon",
                                            queryPath: @"Queries\master_attribute_rayon.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses blok");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_blok",
                                            queryPath: @"Queries\master_attribute_blok.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses cabang");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_cabang",
                                            queryPath: @"Queries\master_attribute_cabang.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses kecamatan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kecamatan",
                                            queryPath: @"Queries\master_attribute_kecamatan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses kelurahan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kelurahan",
                                            queryPath: @"Queries\master_attribute_kelurahan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses dma");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_dma",
                                            queryPath: @"Queries\master_attribute_dma.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses dmz");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_dmz",
                                            queryPath: @"Queries\master_attribute_dmz.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses administrasi lain");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_administrasi_lain",
                                            queryPath: @"Queries\master_tarif_administrasi_lain.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses pemeliharaan lain");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_pemeliharaan_lain",
                                            queryPath: @"Queries\master_tarif_pemeliharaan_lain.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses retribusi lain");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_tarif_retribusi_lain",
                                            queryPath: @"Queries\master_tarif_retribusi_lain.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses kolektif");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kolektif",
                                            queryPath: @"Queries\master_attribute_kolektif.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses sumber air");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_sumber_air",
                                            queryPath: @"Queries\master_attribute_sumber_air.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses merek meter");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_merek_meter",
                                            queryPath: @"Queries\master_attribute_merek_meter.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses kondisi meter");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kondisi_meter",
                                            queryPath: @"Queries\master_attribute_kondisi_meter.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses kelainan");
                                        await Utils.Client(async (conn, trans) =>
                                        {
                                            await conn.ExecuteAsync($"DELETE FROM master_attribute_kelainan WHERE idpdam={settings.IdPdam}",
                                                 transaction: trans);
                                        });
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var cek = await conn.QueryAsync("SELECT column_name FROM information_schema.COLUMNS WHERE table_schema=@schema AND table_name='kelainan'",
                                                new { schema = AppSettings.DBNameBilling }, trans);
                                            if (cek.Any())
                                            {
                                                if (!cek.Where(s => s.column_name == "idkelainan").Any())
                                                {
                                                    await conn.ExecuteAsync("ALTER TABLE kelainan CHANGE COLUMN id idkelainan INT(11)", transaction: trans);
                                                }

                                                if (!cek.Where(s => s.column_name == "kodekelainan").Any())
                                                {
                                                    await conn.ExecuteAsync("ALTER TABLE kelainan ADD COLUMN kodekelainan VARCHAR(10)", transaction: trans);
                                                }

                                                if (!cek.Where(s => s.column_name == "idx").Any())
                                                {
                                                    await conn.ExecuteAsync("ALTER TABLE kelainan ADD COLUMN idx int(11)", transaction: trans);
                                                }

                                                if (!cek.Where(s => s.column_name == "aktif").Any())
                                                {
                                                    await conn.ExecuteAsync("ALTER TABLE kelainan ADD COLUMN aktif varchar(1)", transaction: trans);
                                                }
                                            }
                                        });
                                        await Utils.ClientBacameter(async (conn, trans) =>
                                        {
                                            var kelainan = await conn.QueryAsync(@"SELECT idkelainan,kodekelainan,kelainan,idx,aktif FROM kelainan", transaction: trans);
                                            if (kelainan.Any())
                                            {
                                                await Utils.ClientBilling(async (conn, trans) =>
                                                {
                                                    await conn.ExecuteAsync("DELETE FROM kelainan", transaction: trans);
                                                    await conn.ExecuteAsync("REPLACE INTO kelainan (idkelainan,kodekelainan,kelainan,idx,aktif) VALUES (@idkelainan,@kodekelainan,@kelainan,@idx,@aktif)",
                                                        kelainan, trans);
                                                });
                                            }
                                        });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_kelainan",
                                            queryPath: @"Queries\master_attribute_kelainan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses petugas baca");
                                        await Utils.Client(async (conn, trans) =>
                                        {
                                            await conn.ExecuteAsync($"DELETE FROM master_attribute_petugas_baca WHERE idpdam={settings.IdPdam}",
                                                 transaction: trans);
                                        });
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var cek = await conn.QueryAsync("SELECT column_name FROM information_schema.COLUMNS WHERE table_schema=@schema AND table_name='pembacameter'",
                                                new { schema = AppSettings.DBNameBilling }, trans);
                                            if (cek.Any())
                                            {
                                                if (!cek.Where(s => s.column_name == "idpetugas").Any())
                                                {
                                                    await conn.ExecuteAsync("ALTER TABLE pembacameter ADD COLUMN idpetugas int(11)", transaction: trans);
                                                }

                                                if (!cek.Where(s => s.column_name == "kodepetugas").Any())
                                                {
                                                    await conn.ExecuteAsync("ALTER TABLE pembacameter ADD COLUMN kodepetugas varchar(5)", transaction: trans);
                                                }

                                                if (!cek.Where(s => s.column_name == "aktif").Any())
                                                {
                                                    await conn.ExecuteAsync("ALTER TABLE pembacameter ADD COLUMN aktif varchar(1)", transaction: trans);
                                                }
                                            }
                                        });
                                        await Utils.ClientBacameter(async (conn, trans) =>
                                        {
                                            var petugas = await conn.QueryAsync(@"SELECT idpetugas,kodepetugas,nama,aktif FROM petugasbaca", transaction: trans);
                                            if (petugas.Any())
                                            {
                                                await Utils.ClientBilling(async (conn, trans) =>
                                                {
                                                    await conn.ExecuteAsync("DELETE FROM pembacameter", transaction: trans);
                                                    await conn.ExecuteAsync("REPLACE INTO pembacameter (idpetugas,kodepetugas,nama,aktif) VALUES (@idpetugas,@kodepetugas,@nama,@aktif)",
                                                        petugas, trans);
                                                });
                                            }
                                        });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_petugas_baca",
                                            queryPath: @"Queries\master_attribute_petugas_baca.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses periode");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_periode",
                                            queryPath: @"Queries\master_periode.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_periode_billing",
                                            queryPath: @"Queries\master_periode_billing.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses jadwal baca");
                                        await Utils.ClientBacameter(async (conn, trans) =>
                                        {
                                            var jadwalbaca = await conn.QueryAsync(@"SELECT
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
                                                        await conn.ExecuteAsync(@"REPLACE master_attribute_jadwal_baca (idpdam,idjadwalbaca,idpetugasbaca,idrayon)
                                                    VALUES (@idpdam,@idjadwalbaca,@idpetugasbaca,@idrayon)", data, trans);
                                                    }
                                                });
                                            }
                                        });

                                        ctx.Status("proses loket");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_loket",
                                            queryPath: @"Queries\master_attribute_loket.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses user");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_user",
                                            queryPath: @"Queries\master_user.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses query global");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_query_global",
                                            queryPath: @"Queries\Master\master_query_global.sql");

                                        ctx.Status("proses config list data");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_tipe_permohonan_config_list_data",
                                            queryPath: @"Queries\Master\master_attribute_tipe_permohonan_config_list_data.sql");

                                        ctx.Status("proses tipe pendaftaran sambungan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_tipe_pendaftaran_sambungan",
                                            queryPath: @"Queries\Master\master_attribute_tipe_pendaftaran_sambungan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses sumber pengaduan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_sumber_pengaduan",
                                            queryPath: @"Queries\Master\master_attribute_sumber_pengaduan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });

                                    await Utils.TrackProgress("jenis non air", async () =>
                                    {
                                        ctx.Status("proses jenis non air");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_jenis_nonair",
                                            queryPath: @"Queries\Master\master_attribute_jenis_nonair.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_jenis_nonair_detail",
                                            queryPath: @"Queries\Master\master_attribute_jenis_nonair_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });

                                    await Utils.TrackProgress("tipe permohonan", async () =>
                                    {
                                        ctx.Status("proses tipe permohonan");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_tipe_permohonan",
                                            queryPath: @"Queries\Master\master_attribute_tipe_permohonan.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_tipe_permohonan_detail",
                                            queryPath: @"Queries\Master\master_attribute_tipe_permohonan_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_tipe_permohonan_detail_ba",
                                            queryPath: @"Queries\Master\master_attribute_tipe_permohonan_detail_ba.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_tipe_permohonan_detail_spk",
                                            queryPath: @"Queries\Master\master_attribute_tipe_permohonan_detail_spk.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });

                                    await Utils.TrackProgress("paket material", async () =>
                                    {
                                        ctx.Status("proses paket material");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_material",
                                            queryPath: @"Queries\Master\master_attribute_material.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_material_paket",
                                            queryPath: @"Queries\Master\master_attribute_material_paket.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_material_paket_detail",
                                            queryPath: @"Queries\Master\master_attribute_material_paket_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                    });

                                    await Utils.TrackProgress("paket ongkos", async () =>
                                    {
                                        ctx.Status("proses paket ongkos");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_ongkos",
                                            queryPath: @"Queries\Master\master_attribute_ongkos.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_ongkos_paket",
                                            queryPath: @"Queries\Master\master_attribute_ongkos_paket.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_ongkos_paket_detail",
                                            queryPath: @"Queries\Master\master_attribute_ongkos_paket_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                    });

                                    await Utils.TrackProgress("paket rab", async () =>
                                    {
                                        ctx.Status("proses paket rab");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringLoket,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_paket",
                                            queryPath: @"Queries\Master\master_attribute_paket.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            },
                                            placeholders: new()
                                            {
                                                { "[bsbs]", AppSettings.DBNameBilling }
                                            });
                                    });

                                    await Utils.TrackProgress("report", async () =>
                                    {
                                        ctx.Status("proses label report");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_attribute_label_report",
                                            queryPath: @"Queries\Master\master_attribute_label_report.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses report main group");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_report_maingroup",
                                            queryPath: @"Queries\Master\master_report_maingroup.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses report sub group");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_report_subgroup",
                                            queryPath: @"Queries\Master\master_report_subgroup.sql");

                                        ctx.Status("proses report api");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "report_api",
                                            queryPath: @"Queries\Master\report_api.sql");

                                        ctx.Status("proses report model");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "report_models",
                                            queryPath: @"Queries\Master\report_models.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses report model source");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "report_model_sources",
                                            queryPath: @"Queries\Master\report_model_sources.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses report model sort");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "report_model_sorts",
                                            queryPath: @"Queries\Master\report_model_sorts.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses report model prop");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "report_model_props",
                                            queryPath: @"Queries\Master\report_model_props.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses report model param");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "report_model_params",
                                            queryPath: @"Queries\Master\report_model_params.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });

                                        ctx.Status("proses report filter custom");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "report_filter_custom",
                                            queryPath: @"Queries\Master\report_filter_custom.sql");

                                        ctx.Status("proses report filter custom detail");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringStaging,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "report_filter_custom_detail",
                                            queryPath: @"Queries\Master\report_filter_custom_detail.sql");
                                    });

                                    await Utils.TrackProgress("pelanggan air", async () =>
                                    {
                                        ctx.Status("proses pelanggan air");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_pelanggan_air",
                                            queryPath: @"Queries\master_pelanggan_air.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        ctx.Status("proses pelanggan air detail");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "master_pelanggan_air_detail",
                                            queryPath: @"Queries\master_pelanggan_air_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam }
                                            });
                                        ctx.Status("update latlong pelanggan air");
                                        await Utils.ClientBacameter(async (conn, trans) =>
                                        {
                                            var pel = await conn.QueryAsync("SELECT idpelanggan,latitude,longitude FROM pelanggan", transaction: trans);
                                            if (pel.Any())
                                            {
                                                await Utils.Client(async (conn, trans) =>
                                                {
                                                    await conn.ExecuteAsync($"UPDATE master_pelanggan_air SET latitude=@latitude,longitude=@longitude WHERE idpdam={settings.IdPdam} AND nosamb=@idpelanggan", pel, trans);
                                                });
                                            }
                                        });
                                    });

                                    await Utils.TrackProgress("cleanup data piutang", async () =>
                                    {
                                        ctx.Status("cek golongan");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_golongan.sql");
                                            query = query.Replace("[table]", $"piutang");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek diameter");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_diameter.sql");
                                            query = query.Replace("[table]", $"piutang");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek kelurahan");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kelurahan.sql");
                                            query = query.Replace("[table]", $"piutang");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek kolektif");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kolektif.sql");
                                            query = query.Replace("[table]", $"piutang");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek administrasi lain");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_adm_lain.sql");
                                            query = query.Replace("[table]", $"piutang");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek pemeliharaan lain");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_pem_lain.sql");
                                            query = query.Replace("[table]", $"piutang");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });

                                        ctx.Status("cek retribusi lain");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_ret_lain.sql");
                                            query = query.Replace("[table]", $"piutang");
                                            await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                        });
                                    });

                                    await Utils.TrackProgress("piutang non angsuran", async () =>
                                    {
                                        ctx.Status("proses piutang non angsuran");
                                        var lastId = 0;
                                        await Utils.Client(async (conn, trans) =>
                                        {
                                            lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                                        });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "rekening_air",
                                            queryPath: @"Queries\piutang.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                                { "@lastid", lastId },
                                                { "@flagangsur", 0 },
                                            },
                                            placeholders: new()
                                            {
                                                { "[table]", "piutang" }
                                            });

                                        ctx.Status("proses piutang non angsuran detail");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "rekening_air_detail",
                                            queryPath: @"Queries\piutang_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                                { "@flagangsur", 0 },
                                            },
                                            placeholders: new()
                                            {
                                                { "[table]", "piutang" }
                                            });
                                    }, usingStopwatch: true);

                                    await Utils.TrackProgress("piutang angsuran", async () =>
                                    {
                                        ctx.Status("proses piutang angsuran master");
                                        var lastId = 0;
                                        await Utils.Client(async (conn, trans) =>
                                        {
                                            lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                                        });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "rekening_air",
                                            queryPath: @"Queries\piutang.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                                { "@lastid", lastId },
                                                { "@flagangsur", 1 },
                                            },
                                            placeholders: new()
                                            {
                                                { "[table]", "piutang" }
                                            });

                                        ctx.Status("proses piutang non angsuran master detail");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "rekening_air_detail",
                                            queryPath: @"Queries\piutang_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                                { "@flagangsur", 1 },
                                            },
                                            placeholders: new()
                                            {
                                                { "[table]", "piutang" }
                                            });

                                        ctx.Status("proses piutang angsuran detail");
                                        var lastIdAngsuranDetail = 0;
                                        await Utils.Client(async (conn, trans) =>
                                        {
                                            lastIdAngsuranDetail = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(id),0) FROM rekening_air_angsuran_detail", transaction: trans);
                                        });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "rekening_air_angsuran_detail",
                                            queryPath: @"Queries\piutang_angsuran_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                                { "@lastid", lastIdAngsuranDetail },
                                            });

                                        ctx.Status("proses piutang angsuran");
                                        var lastIdAngsuran = 0;
                                        var jnsNonair = 0;
                                        await Utils.Client(async (conn, trans) =>
                                        {
                                            lastIdAngsuran = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idangsuran),0) FROM rekening_air_angsuran", transaction: trans);
                                            jnsNonair = await conn.QueryFirstOrDefaultAsync<int>($"SELECT idjenisnonair FROM master_attribute_jenis_nonair WHERE idpdam = {settings.IdPdam} AND kodejenisnonair = 'JNS-36' AND flaghapus = 0", transaction: trans);
                                        });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "rekening_air_angsuran",
                                            queryPath: @"Queries\piutang_angsuran.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                                { "@lastid", lastIdAngsuran },
                                                { "@jnsnonair", jnsNonair },
                                            });

                                        ctx.Status("update jumlah termin");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var termin = await conn.QueryAsync(@"
                                                DROP TEMPORARY TABLE IF EXISTS temp_dataawal_piutang_termin;
                                                CREATE TEMPORARY TABLE temp_dataawal_piutang_termin AS
                                                SELECT periode,nosamb,COUNT(*) AS termin
                                                FROM piutang
                                                WHERE kode <> CONCAT(periode,'.',nosamb) AND SUBSTRING_INDEX(kode, '.', -1) <> 0
                                                GROUP BY periode,nosamb;

                                                SELECT
                                                rek.periode,
                                                rek.nosamb,
                                                ter.termin
                                                FROM piutang rek
                                                JOIN temp_dataawal_piutang_termin ter ON ter.periode = rek.periode AND ter.nosamb = rek.nosamb
                                                WHERE rek.kode = CONCAT(rek.periode,'.',rek.nosamb) AND rek.flagangsur = 1;
                                                ", transaction: trans);

                                            await Utils.Client(async (conn, trans) =>
                                            {
                                                foreach (var t in termin)
                                                {
                                                    await conn.ExecuteAsync(@"
                                                        UPDATE rekening_air_angsuran
                                                        SET jumlahtermin = @termin
                                                        WHERE idpdam = @idpdam
                                                        AND SUBSTRING_INDEX(SUBSTRING_INDEX(noangsuran, '.', 2), '.', -1) = @periode
                                                        AND SUBSTRING_INDEX(noangsuran, '.', -1) = @nosamb",
                                                        new
                                                        {
                                                            termin = t.termin,
                                                            idpdam = settings.IdPdam,
                                                            periode = t.periode,
                                                            nosamb = t.nosamb
                                                        },
                                                        trans);
                                                }
                                            });
                                        });

                                        //ctx.Status("update jumlah uang muka");

                                        //sambungkan dengan rekening air

                                    }, usingStopwatch: true);

                                    await Utils.TrackProgress("piutang angsur lunas", async () =>
                                    {
                                        ctx.Status("proses piutang angsuran master");
                                        var lastId = 0;
                                        await Utils.Client(async (conn, trans) =>
                                        {
                                            lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                                        });
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "rekening_air",
                                            queryPath: @"Queries\piutang.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                                { "@lastid", lastId },
                                                { "@flagangsur", 1 },
                                            },
                                            placeholders: new()
                                            {
                                                { "[table]", "piutang_angsurlunas" }
                                            });

                                        ctx.Status("proses bayar angsuran master detail");
                                        await Utils.BulkCopy(
                                            sConnectionStr: AppSettings.ConnectionStringBilling,
                                            tConnectionStr: AppSettings.ConnectionString,
                                            tableName: "rekening_air_detail",
                                            queryPath: @"Queries\piutang_detail.sql",
                                            parameters: new()
                                            {
                                                { "@idpdam", settings.IdPdam },
                                                { "@flagangsur", 1 },
                                            },
                                            placeholders: new()
                                            {
                                                { "[table]", "piutang_angsurlunas" }
                                            });
                                    }, usingStopwatch: true);

                                    #region bayar

                                    IEnumerable<string?> tahunBayar = [];
                                    await Utils.ClientBilling(async (conn, trans) =>
                                    {
                                        tahunBayar = await conn.QueryAsync<string?>("SELECT RIGHT(table_name, 4) FROM information_schema.TABLES WHERE table_schema=@table_schema AND table_name RLIKE 'bayar[0-9]{4}'",
                                            new { table_schema = AppSettings.DBNameBilling }, trans);
                                    });

                                    foreach (var tahun in tahunBayar)
                                    {
                                        await Utils.TrackProgress($"cleanup data bayar{tahun}", async () =>
                                        {
                                            ctx.Status($"Cek golongan");
                                            await Utils.ClientBilling(async (conn, trans) =>
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_golongan.sql");
                                                query = query.Replace("[table]", $"bayar{tahun}");
                                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                            });

                                            ctx.Status($"Cek diameter");
                                            await Utils.ClientBilling(async (conn, trans) =>
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_diameter.sql");
                                                query = query.Replace("[table]", $"bayar{tahun}");
                                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                            });

                                            ctx.Status($"Cek kelurahan");
                                            await Utils.ClientBilling(async (conn, trans) =>
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kelurahan.sql");
                                                query = query.Replace("[table]", $"bayar{tahun}");
                                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                            });

                                            ctx.Status($"Cek kolektif");
                                            await Utils.ClientBilling(async (conn, trans) =>
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kolektif.sql");
                                                query = query.Replace("[table]", $"bayar{tahun}");
                                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                            });

                                            ctx.Status($"Cek administrasi lain");
                                            await Utils.ClientBilling(async (conn, trans) =>
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_adm_lain.sql");
                                                query = query.Replace("[table]", $"bayar{tahun}");
                                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                            });

                                            ctx.Status($"Cek pemeliharaan lain");
                                            await Utils.ClientBilling(async (conn, trans) =>
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_pem_lain.sql");
                                                query = query.Replace("[table]", $"bayar{tahun}");
                                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                            });

                                            ctx.Status($"Cek retribusi lain");
                                            await Utils.ClientBilling(async (conn, trans) =>
                                            {
                                                var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_ret_lain.sql");
                                                query = query.Replace("[table]", $"bayar{tahun}");
                                                await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                            });
                                        });

                                        var lastId = 0;
                                        await Utils.Client(async (conn, trans) =>
                                        {
                                            lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                                        });

                                        ctx.Status($"proses bayar{tahun} non angsuran");
                                        await Utils.TrackProgress($"bayar{tahun} non angsuran", async () =>
                                        {
                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringBilling,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air",
                                                queryPath: @"Queries\bayar.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam },
                                                    { "@lastid", lastId },
                                                    { "@flagangsur", 0 },
                                                },
                                                placeholders: new()
                                                {
                                                    { "[table]", $"bayar{tahun}" }
                                                });
                                        }, usingStopwatch: true);

                                        ctx.Status($"proses bayar{tahun} detail non angsuran");
                                        await Utils.TrackProgress($"bayar{tahun} detail non angsuran", async () =>
                                        {
                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringBilling,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air_detail",
                                                queryPath: @"Queries\bayar_detail.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam },
                                                    { "@flagangsur", 0 },
                                                },
                                                placeholders: new()
                                                {
                                                    { "[table]", $"bayar{tahun}" }
                                                });
                                        }, usingStopwatch: true);

                                        ctx.Status($"proses bayar{tahun} transaksi non angsuran");
                                        await Utils.TrackProgress($"bayar{tahun} transaksi non angsuran", async () =>
                                        {
                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringBilling,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air_transaksi",
                                                queryPath: @"Queries\bayar_transaksi.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam }
                                                },
                                                placeholders: new()
                                                {
                                                    { "[table]", $"bayar{tahun}" },
                                                    { "[dbloket]", AppSettings.DBNameLoket }
                                                });
                                        }, usingStopwatch: true);
                                    }

                                    foreach (var tahun in tahunBayar)
                                    {
                                        var lastId = 0;
                                        await Utils.Client(async (conn, trans) =>
                                        {
                                            lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                                        });

                                        ctx.Status($"proses bayar{tahun} angsuran");
                                        await Utils.TrackProgress($"bayar{tahun} angsuran", async () =>
                                        {
                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringBilling,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air",
                                                queryPath: @"Queries\bayar.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam },
                                                    { "@lastid", lastId },
                                                    { "@flagangsur", 1 },
                                                },
                                                placeholders: new()
                                                {
                                                    { "[table]", $"bayar{tahun}" }
                                                });
                                        }, usingStopwatch: true);

                                        ctx.Status($"proses bayar{tahun} detail angsuran");
                                        await Utils.TrackProgress($"bayar{tahun} detail angsuran", async () =>
                                        {
                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringBilling,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air_detail",
                                                queryPath: @"Queries\bayar_detail.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam },
                                                    { "@flagangsur", 1 },
                                                },
                                                placeholders: new()
                                                {
                                                    { "[table]", $"bayar{tahun}" }
                                                });
                                        }, usingStopwatch: true);

                                        ctx.Status($"proses bayar{tahun} angsuran detail");
                                        await Utils.TrackProgress($"bayar{tahun} angsuran detail", async () =>
                                        {
                                            var lastIdAngsuranDetail = 0;
                                            await Utils.Client(async (conn, trans) =>
                                            {
                                                lastIdAngsuranDetail = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(id),0) FROM rekening_air_angsuran_detail", transaction: trans);
                                            });
                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringBilling,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air_angsuran_detail",
                                                queryPath: @"Queries\bayar_angsuran_detail.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam },
                                                    { "@lastid", lastIdAngsuranDetail },
                                                });
                                        });

                                        ctx.Status($"proses bayar{tahun} angsuran");
                                        await Utils.TrackProgress($"bayar{tahun} angsuran", async () =>
                                        {
                                            var lastIdAngsuran = 0;
                                            var jnsNonair = 0;
                                            await Utils.Client(async (conn, trans) =>
                                            {
                                                lastIdAngsuran = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idangsuran),0) FROM rekening_air_angsuran", transaction: trans);
                                                jnsNonair = await conn.QueryFirstOrDefaultAsync<int>($"SELECT idjenisnonair FROM master_attribute_jenis_nonair WHERE idpdam = {settings.IdPdam} AND kodejenisnonair = 'JNS-36' AND flaghapus = 0", transaction: trans);
                                            });
                                            await Utils.BulkCopy(
                                                sConnectionStr: AppSettings.ConnectionStringBilling,
                                                tConnectionStr: AppSettings.ConnectionString,
                                                tableName: "rekening_air_angsuran",
                                                queryPath: @"Queries\bayar_angsuran.sql",
                                                parameters: new()
                                                {
                                                    { "@idpdam", settings.IdPdam },
                                                    { "@lastid", lastIdAngsuran },
                                                    { "@jnsnonair", jnsNonair },
                                                });
                                        });
                                    }

                                    #endregion
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
    }
}
