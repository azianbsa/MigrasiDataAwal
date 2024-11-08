using Spectre.Console.Cli;
using Spectre.Console;
using System.Diagnostics;
using Dapper;
using Migrasi.Helpers;

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
            settings.IdPdam ??= AnsiConsole.Ask<int>("ID PDAM :");
            settings.NamaPaket ??= AnsiConsole.Prompt(
                    new SelectionPrompt<Paket>()
                    .Title("Pilih paket :")
                    .AddChoices([Paket.Bacameter, Paket.Basic]));

            switch (settings.NamaPaket)
            {
                case Paket.Bacameter:
                    {
                        var bbPeriode = AnsiConsole.Ask<int>("Periode data dari (yyyyMM) :");

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
                            .AddRow("Paket", settings.NamaPaket.ToString()!)
                            .AddRow("Bacameter", AppSettings.DBNameBacameter)
                            .AddRow("Billing", AppSettings.DBNameBilling)
                            .AddRow("Periode data dari", bbPeriode.ToString()!)
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
                                    ctx.Status = "Clean redundan data pelanggan bsbs";
                                    #region Clean redundan data pelanggan bsbs

                                    Utils.WriteLogMessage("Tambah primary key id pelanggan");
                                    await Utils.ClientBilling(async (conn, trans) =>
                                    {
                                        var cek = await conn.QueryFirstOrDefaultAsync<int?>("SELECT 1 FROM information_schema.COLUMNS WHERE table_schema=@schema AND table_name='pelanggan' AND column_name='id'",
                                            new { schema = AppSettings.DBNameBilling }, trans);
                                        if (cek is null)
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\tambah_field_id_tabel_pelanggan.sql");
                                            await conn.ExecuteAsync(query, transaction: trans);
                                        }
                                    });

                                    Utils.WriteLogMessage("Cek pelanggan golongan");
                                    await Utils.ClientBilling(async (conn, trans) =>
                                    {
                                        var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_golongan.sql");
                                        query = query.Replace("[table]", "pelanggan");
                                        await conn.ExecuteAsync(query, transaction: trans);
                                    });

                                    Utils.WriteLogMessage("Cek pelanggan diameter");
                                    await Utils.ClientBilling(async (conn, trans) =>
                                    {
                                        var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_diameter.sql");
                                        query = query.Replace("[table]", "pelanggan");
                                        await conn.ExecuteAsync(query, transaction: trans);
                                    });

                                    Utils.WriteLogMessage("Cek pelanggan merek meter");
                                    await Utils.ClientBilling(async (conn, trans) =>
                                    {
                                        var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_merek_meter.sql");
                                        await conn.ExecuteAsync(query, transaction: trans);
                                    });

                                    Utils.WriteLogMessage("Cek pelanggan kelurahan");
                                    await Utils.ClientBilling(async (conn, trans) =>
                                    {
                                        var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kelurahan.sql");
                                        query = query.Replace("[table]", "pelanggan");
                                        await conn.ExecuteAsync(query, transaction: trans);
                                    });

                                    Utils.WriteLogMessage("Cek pelanggan kolektif");
                                    await Utils.ClientBilling(async (conn, trans) =>
                                    {
                                        var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kolektif.sql");
                                        query = query.Replace("[table]", "pelanggan");
                                        await conn.ExecuteAsync(query, transaction: trans);
                                    });

                                    Utils.WriteLogMessage("Cek pelanggan sumber air");
                                    await Utils.ClientBilling(async (conn, trans) =>
                                    {
                                        var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_sumber_air.sql");
                                        await conn.ExecuteAsync(query, transaction: trans);
                                    });

                                    Utils.WriteLogMessage("Cek pelanggan blok");
                                    await Utils.ClientBilling(async (conn, trans) =>
                                    {
                                        var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_blok.sql");
                                        await conn.ExecuteAsync(query, transaction: trans);
                                    });

                                    Utils.WriteLogMessage("Cek pelanggan kondisi meter");
                                    await Utils.ClientBilling(async (conn, trans) =>
                                    {
                                        var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kondisi_meter.sql");
                                        await conn.ExecuteAsync(query, transaction: trans);
                                    });

                                    Utils.WriteLogMessage("Cek pelanggan administrasi lain");
                                    await Utils.ClientBilling(async (conn, trans) =>
                                    {
                                        var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_adm_lain.sql");
                                        query = query.Replace("[table]", "pelanggan");
                                        await conn.ExecuteAsync(query, transaction: trans);
                                    });

                                    Utils.WriteLogMessage("Cek pelanggan pemeliharaan lain");
                                    await Utils.ClientBilling(async (conn, trans) =>
                                    {
                                        var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_pem_lain.sql");
                                        query = query.Replace("[table]", "pelanggan");
                                        await conn.ExecuteAsync(query, transaction: trans);
                                    });

                                    Utils.WriteLogMessage("Cek pelanggan retribusi lain");
                                    await Utils.ClientBilling(async (conn, trans) =>
                                    {
                                        var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_ret_lain.sql");
                                        query = query.Replace("[table]", "pelanggan");
                                        await conn.ExecuteAsync(query, transaction: trans);
                                    });

                                    #endregion

                                    ctx.Status = "Proses data master";
                                    #region Proses data master

                                    Utils.WriteLogMessage("Proses flag");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        queryPath: @"Queries\master_attribute_flag.sql",
                                        tableName: "master_attribute_flag",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses status");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_status",
                                        queryPath: @"Queries\master_attribute_status.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses jenis bangunan");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_jenis_bangunan",
                                        queryPath: @"Queries\master_attribute_jenis_bangunan.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses kepemilikan");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_kepemilikan",
                                        queryPath: @"Queries\master_attribute_kepemilikan.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses pekerjaan");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_pekerjaan",
                                        queryPath: @"Queries\master_attribute_pekerjaan.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses peruntukan");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_peruntukan",
                                        queryPath: @"Queries\master_attribute_peruntukan.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses jenis pipa");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_jenis_pipa",
                                        queryPath: @"Queries\master_attribute_jenis_pipa.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses kwh");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_kwh",
                                        queryPath: @"Queries\master_attribute_kwh.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses golongan");
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

                                    Utils.WriteLogMessage("Proses diameter");
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

                                    Utils.WriteLogMessage("Proses wilayah");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_wilayah",
                                        queryPath: @"Queries\master_attribute_wilayah.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses area");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_area",
                                        queryPath: @"Queries\master_attribute_area.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses rayon");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_rayon",
                                        queryPath: @"Queries\master_attribute_rayon.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses blok");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_blok",
                                        queryPath: @"Queries\master_attribute_blok.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses cabang");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_cabang",
                                        queryPath: @"Queries\master_attribute_cabang.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses kecamatan");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_kecamatan",
                                        queryPath: @"Queries\master_attribute_kecamatan.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses kelurahan");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_kelurahan",
                                        queryPath: @"Queries\master_attribute_kelurahan.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses dma");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_dma",
                                        queryPath: @"Queries\master_attribute_dma.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses dmz");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_dmz",
                                        queryPath: @"Queries\master_attribute_dmz.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses administrasi lain");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_tarif_administrasi_lain",
                                        queryPath: @"Queries\master_tarif_administrasi_lain.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses pemeliharaan lain");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_tarif_pemeliharaan_lain",
                                        queryPath: @"Queries\master_tarif_pemeliharaan_lain.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses retribusi lain");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_tarif_retribusi_lain",
                                        queryPath: @"Queries\master_tarif_retribusi_lain.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses kolektif");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_kolektif",
                                        queryPath: @"Queries\master_attribute_kolektif.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses sumber air");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_sumber_air",
                                        queryPath: @"Queries\master_attribute_sumber_air.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses merek meter");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_merek_meter",
                                        queryPath: @"Queries\master_attribute_merek_meter.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses kondisi meter");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_kondisi_meter",
                                        queryPath: @"Queries\master_attribute_kondisi_meter.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses kelainan");
                                    await Utils.ClientBacameter(async (conn, trans) =>
                                    {
                                        var kelainan = await conn.QueryAsync(@"SELECT kelainan FROM kelainan", transaction: trans);
                                        if (kelainan.Any())
                                        {
                                            await Utils.ClientBilling(async (conn, trans) =>
                                            {
                                                await conn.ExecuteAsync("REPLACE INTO kelainan (kelainan) VALUES (@kelainan)",
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

                                    Utils.WriteLogMessage("Proses petugas baca");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_attribute_petugas_baca",
                                        queryPath: @"Queries\master_attribute_petugas_baca.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses periode");
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

                                    Utils.WriteLogMessage("Proses pelanggan air");
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_pelanggan_air",
                                        queryPath: @"Queries\master_pelanggan_air.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });
                                    await Utils.BulkCopy(
                                        sConnectionStr: AppSettings.ConnectionStringBilling,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "master_pelanggan_air_detail",
                                        queryPath: @"Queries\master_pelanggan_air_detail.sql",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam }
                                        });

                                    Utils.WriteLogMessage("Proses jadwal baca");
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

                                    #endregion

                                    for (int i = bbPeriode; i < bbPeriode + 4; i++)
                                    {
                                        ctx.Status = $"Proses data drd {(i - bbPeriode) + 1}/4";
                                        Utils.WriteLogMessage("Cek bsbs golongan");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_golongan.sql");
                                            query = query.Replace("[table]", $"drd{i}");
                                            await conn.ExecuteAsync(query, transaction: trans);
                                        });

                                        Utils.WriteLogMessage("Cek bsbs diameter");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_diameter.sql");
                                            query = query.Replace("[table]", $"drd{i}");
                                            await conn.ExecuteAsync(query, transaction: trans);
                                        });

                                        Utils.WriteLogMessage("Cek bsbs kelurahan");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kelurahan.sql");
                                            query = query.Replace("[table]", $"drd{i}");
                                            await conn.ExecuteAsync(query, transaction: trans);
                                        });

                                        Utils.WriteLogMessage("Cek bsbs kolektif");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_kolektif.sql");
                                            query = query.Replace("[table]", $"drd{i}");
                                            await conn.ExecuteAsync(query, transaction: trans);
                                        });

                                        Utils.WriteLogMessage("Cek bsbs administrasi lain");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_adm_lain.sql");
                                            query = query.Replace("[table]", $"drd{i}");
                                            await conn.ExecuteAsync(query, transaction: trans);
                                        });

                                        Utils.WriteLogMessage("Cek bsbs pemeliharaan lain");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_pem_lain.sql");
                                            query = query.Replace("[table]", $"drd{i}");
                                            await conn.ExecuteAsync(query, transaction: trans);
                                        });

                                        Utils.WriteLogMessage("Cek bsbs retribusi lain");
                                        await Utils.ClientBilling(async (conn, trans) =>
                                        {
                                            var query = await File.ReadAllTextAsync(@"Queries\Patches\data_cleanup_ret_lain.sql");
                                            query = query.Replace("[table]", $"drd{i}");
                                            await conn.ExecuteAsync(query, transaction: trans);
                                        });

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
                                    }
                                });

                            sw.Stop();
                            AnsiConsole.MarkupLine($"[bold green]Migrasi data bacameter finish (elapsed {sw.Elapsed})[/]");
                        }
                        catch (Exception)
                        {
                            throw;
                        }

                        break;
                    }
                case Paket.Basic:
                    {

                        break;
                    }
                default:
                    break;
            }

            return 0;
        }
    }
}
