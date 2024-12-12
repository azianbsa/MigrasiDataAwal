using Migrasi.Helpers;
using Spectre.Console.Cli;
using Spectre.Console;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using MySqlConnector;

namespace Migrasi.Commands
{
    public class PiutangCommand : AsyncCommand<PiutangCommand.Settings>
    {
        public class Settings : CommandSettings
        {
            [CommandOption("-i|--idpdam")]
            public int? IdPdam { get; set; }
            public int? IdPdamCopy { get; set; }
        }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            settings.IdPdam ??= AnsiConsole.Ask<int>("ID:");

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
                .AddRow("Environment", AppSettings.Environment.ToString()));

            if (!Utils.ConfirmationPrompt("Yakin untuk melanjutkan?"))
            {
                return 0;
            }

            try
            {
                await AnsiConsole.Status()
                    .StartAsync("Sedang diproses...",async ctx =>
                    {
                        await Utils.TrackProgress("piutang|rekening_air", async () =>
                        {
                            var lastId = 0;
                            await Utils.Client(async (conn, trans) =>
                            {
                                lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                            });

                            await Utils.BulkCopyV2(
                                sConnectionStr: AppSettings.ConnectionStringLoket,
                                tConnectionStr: AppSettings.ConnectionString,
                                tableName: "rekening_air",
                                query: @"
                                DROP TEMPORARY TABLE IF EXISTS temp_dataawal_periode;
                                CREATE TEMPORARY TABLE temp_dataawal_periode (
                                    idperiode INT,
                                    periode VARCHAR(10),
                                    INDEX idx_temp_dataawal_periode_periode (periode)
                                );
                                INSERT INTO temp_dataawal_periode
                                SELECT
                                @idperiode := @idperiode+1 AS idperiode,
                                periode
                                FROM [bsbs].periode
                                ,(SELECT @idperiode := 0) AS idperiode
                                ORDER BY periode;

                                SELECT
                                 @idpdam,
                                 @id := @id+1 AS idrekeningair,
                                 pel.id AS idpelangganair,
                                 per.idperiode AS idperiode,
                                 gol.id AS idgolongan,
                                 dia.id AS iddiameter,
                                 1 AS idjenispipa,
                                 1 AS idkwh,
                                 ray.id AS idrayon,
                                 kel.id AS idkelurahan,
                                 kol.id AS idkolektif,
                                 adm.id AS idadministrasilain,
                                 pem.id AS idpemeliharaanlain,
                                 ret.id AS idretribusilain,
                                 1 AS idstatus,
                                 rek.flag AS idflag,
                                 rek.stanlalu AS stanlalu,
                                 IFNULL(rek.stanskrg, 0) AS stanskrg,
                                 IFNULL(rek.stanangkat, 0) AS stanangkat,
                                 IFNULL(rek.pakai, 0) AS pakai,
                                 0 AS pakaikalkulasi,
                                 IFNULL(rek.biayapemakaian, 0) AS biayapemakaian,
                                 IFNULL(rek.administrasi, 0) AS administrasi,
                                 IFNULL(rek.pemeliharaan, 0) AS pemeliharaan,
                                 IFNULL(rek.retribusi, 0) AS retribusi,
                                 IFNULL(rek.pelayanan, 0) AS pelayanan,
                                 IFNULL(rek.airlimbah, 0) AS airlimbah,
                                 IFNULL(rek.dendapakai0, 0) AS dendapakai0,
                                 IFNULL(rek.administrasilain, 0) AS administrasilain,
                                 IFNULL(rek.pemeliharaanlain, 0) AS pemeliharaanlain,
                                 IFNULL(rek.retribusilain, 0) AS retribusilain,
                                 IFNULL(rek.ppn, 0) AS ppn,
                                 IFNULL(rek.meterai, 0) AS meterai,
                                 IFNULL(rek.rekair, 0) AS rekair,
                                 IFNULL(rek.dendatunggakan, 0) AS denda,
                                 0 AS diskon,
                                 0 AS deposit,
                                 IFNULL(rek.total, 0) AS total,
                                 0 AS hapussecaraakuntansi,
                                 NULL AS waktuhapussecaraakuntansi,
                                 NULL AS iddetailcyclepembacaan,
                                 NULL AS tglpenentuanbaca,
                                 1 AS flagbaca,
                                 0 AS metodebaca,
                                 DATE(NOW()) AS waktubaca,
                                 DATE_FORMAT(NOW(), '%H:%i:%s') AS jambaca,
                                 pbc.kodepetugas AS petugasbaca,
                                 kln.idkelainan AS kelainan,
                                 0 AS stanbaca,
                                 DATE(NOW()) AS waktukirimhasilbaca,
                                 DATE_FORMAT(NOW(), '%H:%i:%s') AS jamkirimhasilbaca,
                                 NULL AS memolapangan,
                                 NULL AS lampiran,
                                 0 AS taksasi,
                                 0 AS taksir,
                                 0 AS flagrequestbacaulang,
                                 NULL AS waktuupdaterequestbacaulang,
                                 1 AS flagkoreksi,
                                 DATE(NOW()) AS waktukoreksi,
                                 DATE_FORMAT(NOW(), '%H:%i:%s') AS jamkoreksi,
                                 1 AS flagverifikasi,
                                 DATE(NOW()) AS waktuverifikasi,
                                 DATE_FORMAT(NOW(), '%H:%i:%s') AS jamverifikasi,
                                 1 AS flagpublish,
                                 DATE(NOW()) AS waktupublish,
                                 DATE_FORMAT(NOW(), '%H:%i:%s') AS jampublish,
                                 NULL AS latitude,
                                 NULL AS longitude,
                                 NULL AS latitudebulanlalu,
                                 NULL AS longitudebulanlalu,
                                 1 AS adafotometer,
                                 0 AS adafotorumah,
                                 0 AS adavideo,
                                 0 AS flagminimumpakai,
                                 0 AS pakaibulanlalu,
                                 0 AS pakai2bulanlalu,
                                 0 AS pakai3bulanlalu,
                                 0 AS pakai4bulanlalu,
                                 0 AS persentasebulanlalu,
                                 0 AS persentase2bulanlalu,
                                 0 AS persentase3bulanlalu,
                                 NULL AS kelainanbulanlalu,
                                 NULL AS kelainan2bulanlalu,
                                 rek.flagangsur AS flagangsur,
                                 NULL AS idangsuran,
                                 NULL AS idmodule,
                                 0 AS flagkoreksibilling,
                                 rek.tglmulaidenda AS tglmulaidenda1,
                                 rek.tglmulaidenda2 AS tglmulaidenda2,
                                 rek.tglmulaidenda3 AS tglmulaidenda3,
                                 rek.tglmulaidenda4 AS tglmulaidenda4,
                                 rek.tglmulaidendaperbulan AS tglmulaidendaperbulan,
                                 0 AS flaghasbeenpublish,
                                 0 AS flagdrdsusulan,
                                 NULL AS waktudrdsusulan,
                                 NOW() AS waktuupdate,
                                 0 AS flaghapus
                                FROM piutang rek
                                 JOIN [bsbs].pelanggan pel ON pel.nosamb = rek.nosamb
                                 JOIN temp_dataawal_periode per ON per.periode = rek.periode
                                 LEFT JOIN [bsbs].golongan gol ON gol.kodegol = rek.kodegol AND gol.aktif = 1
                                 LEFT JOIN [bsbs].diameter dia ON dia.kodediameter = rek.kodediameter AND dia.aktif = 1
                                 LEFT JOIN [bsbs].rayon ray ON ray.koderayon = rek.koderayon
                                 LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan = pel.kodekelurahan
                                 LEFT JOIN [bsbs].kolektif kol ON kol.kodekolektif = rek.kodekolektif
                                 LEFT JOIN [bsbs].byadministrasi_lain adm ON adm.kode = rek.kodeadministrasilain
                                 LEFT JOIN [bsbs].bypemeliharaan_lain pem ON pem.kode = rek.kodepemeliharaanlain
                                 LEFT JOIN [bsbs].byretribusi_lain ret ON ret.kode = rek.koderetribusilain
                                 LEFT JOIN [bsbs].pembacameter pbc ON pbc.nama = TRIM(SUBSTRING_INDEX(rek.pembacameter, '(', 1))
                                 LEFT JOIN [bsbs].kelainan kln ON kln.kelainan = rek.kelainan
                                 ,(SELECT @id := @lastid) AS id
                                 WHERE rek.kode = CONCAT(rek.periode, '.', rek.nosamb) AND rek.flagangsur = 0",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                    { "@lastid", lastId },
                                },
                                placeholders: new()
                                {
                                    { "[bsbs]", AppSettings.DBNameBilling },
                                });
                        }, usingStopwatch: true);

                        await Utils.TrackProgress("piutang detail|rekening_air_detail", async () =>
                        {
                            await Utils.BulkCopyV2(
                                sConnectionStr: AppSettings.ConnectionStringLoket,
                                tConnectionStr: AppSettings.ConnectionString,
                                tableName: "rekening_air_detail",
                                query: @"
                                DROP TEMPORARY TABLE IF EXISTS temp_dataawal_periode;
                                CREATE TEMPORARY TABLE temp_dataawal_periode (
                                    idperiode INT,
                                    periode VARCHAR(10),
                                    INDEX idx_temp_dataawal_periode_periode (periode)
                                );
                                INSERT INTO temp_dataawal_periode
                                SELECT
                                @idperiode := @idperiode+1 AS idperiode,
                                periode
                                FROM [bsbs].periode
                                ,(SELECT @idperiode := 0) AS idperiode
                                ORDER BY periode;

                                SELECT
                                 @idpdam,
                                 pel.id AS idpelangganair,
                                 per.idperiode AS idperiode,
                                 IFNULL(rek.blok1, 0) AS blok1,
                                 IFNULL(rek.blok2, 0) AS blok2,
                                 IFNULL(rek.blok3, 0) AS blok3,
                                 IFNULL(rek.blok4, 0) AS blok4,
                                 IFNULL(rek.blok5, 0) AS blok5,
                                 IFNULL(rek.prog1, 0) AS prog1,
                                 IFNULL(rek.prog2, 0) AS prog2,
                                 IFNULL(rek.prog3, 0) AS prog3,
                                 IFNULL(rek.prog4, 0) AS prog4,
                                 IFNULL(rek.prog5, 0) AS prog5
                                FROM
                                 piutang rek
                                 JOIN [bsbs].pelanggan pel ON pel.nosamb = rek.nosamb
                                 JOIN temp_dataawal_periode per ON per.periode = rek.periode
                                 WHERE rek.kode = CONCAT(rek.periode, '.', rek.nosamb) AND rek.flagangsur = 0",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                },
                                placeholders: new()
                                {
                                    { "[bsbs]", AppSettings.DBNameBilling },
                                });
                        }, usingStopwatch: true);

                        IEnumerable<string?> bayarTahun = [];
                        await Utils.ClientLoket(async (conn, trans) =>
                        {
                            bayarTahun = await conn.QueryAsync<string?>(@"
                            SELECT RIGHT(table_name, 4) FROM information_schema.TABLES WHERE table_schema=@table_schema AND table_name RLIKE 'bayar[0-9]{4}'",
                            new { table_schema = AppSettings.DBNameLoket }, trans);
                        });

                        foreach (var bayar in bayarTahun)
                        {
                            IEnumerable<int>? listPeriode = [];
                            await Utils.ClientLoket(async (conn, trans) =>
                            {
                                listPeriode = await conn.QueryAsync<int>($@"SELECT periode FROM bayar{bayar} GROUP BY periode", transaction: trans);
                            });

                            foreach (var periode in listPeriode)
                            {
                                await Utils.TrackProgress($"bayar{bayar}-{periode}|rekening_air", async () =>
                                {
                                    var lastId = 0;
                                    await Utils.Client(async (conn, trans) =>
                                    {
                                        lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                                    });

                                    await Utils.BulkCopyV2(
                                        sConnectionStr: AppSettings.ConnectionStringLoket,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "rekening_air",
                                        query: @"
                                        DROP TEMPORARY TABLE IF EXISTS temp_dataawal_periode;
                                        CREATE TEMPORARY TABLE temp_dataawal_periode (
                                            idperiode INT,
                                            periode VARCHAR(10),
                                            INDEX idx_temp_dataawal_periode_periode (periode)
                                        );
                                        INSERT INTO temp_dataawal_periode
                                        SELECT
                                        @idperiode := @idperiode+1 AS idperiode,
                                        periode
                                        FROM [bsbs].periode
                                        ,(SELECT @idperiode := 0) AS idperiode
                                        ORDER BY periode;

                                        SELECT
                                         @idpdam,
                                         @id := @id+1 AS idrekeningair,
                                         pel.id AS idpelangganair,
                                         per.idperiode AS idperiode,
                                         gol.id AS idgolongan,
                                         dia.id AS iddiameter,
                                         1 AS idjenispipa,
                                         1 AS idkwh,
                                         ray.id AS idrayon,
                                         kel.id AS idkelurahan,
                                         kol.id AS idkolektif,
                                         adm.id AS idadministrasilain,
                                         pem.id AS idpemeliharaanlain,
                                         ret.id AS idretribusilain,
                                         1 AS idstatus,
                                         1 AS idflag,
                                         rek.stanlalu AS stanlalu,
                                         IFNULL(rek.stanskrg, 0) AS stanskrg,
                                         IFNULL(rek.stanangkat, 0) AS stanangkat,
                                         IFNULL(rek.pakai, 0) AS pakai,
                                         0 AS pakaikalkulasi,
                                         IFNULL(rek.biayapemakaian, 0) AS biayapemakaian,
                                         IFNULL(rek.administrasi, 0) AS administrasi,
                                         IFNULL(rek.pemeliharaan, 0) AS pemeliharaan,
                                         IFNULL(rek.retribusi, 0) AS retribusi,
                                         IFNULL(rek.pelayanan, 0) AS pelayanan,
                                         IFNULL(rek.airlimbah, 0) AS airlimbah,
                                         IFNULL(rek.dendapakai0, 0) AS dendapakai0,
                                         IFNULL(rek.administrasilain, 0) AS administrasilain,
                                         IFNULL(rek.pemeliharaanlain, 0) AS pemeliharaanlain,
                                         IFNULL(rek.retribusilain, 0) AS retribusilain,
                                         IFNULL(rek.ppn, 0) AS ppn,
                                         IFNULL(rek.meterai, 0) AS meterai,
                                         IFNULL(rek.rekair, 0) AS rekair,
                                         IFNULL(rek.dendatunggakan, 0) AS denda,
                                         0 AS diskon,
                                         0 AS deposit,
                                         IFNULL(rek.total, 0) AS total,
                                         0 AS hapussecaraakuntansi,
                                         NULL AS waktuhapussecaraakuntansi,
                                         NULL AS iddetailcyclepembacaan,
                                         NULL AS tglpenentuanbaca,
                                         1 AS flagbaca,
                                         0 AS metodebaca,
                                         DATE(NOW()) AS waktubaca,
                                         DATE_FORMAT(NOW(), '%H:%i:%s') AS jambaca,
                                         pbc.kodepetugas AS petugasbaca,
                                         kln.idkelainan AS kelainan,
                                         0 AS stanbaca,
                                         DATE(NOW()) AS waktukirimhasilbaca,
                                         DATE_FORMAT(NOW(), '%H:%i:%s') AS jamkirimhasilbaca,
                                         NULL AS memolapangan,
                                         NULL AS lampiran,
                                         0 AS taksasi,
                                         0 AS taksir,
                                         0 AS flagrequestbacaulang,
                                         NULL AS waktuupdaterequestbacaulang,
                                         1 AS flagkoreksi,
                                         DATE(NOW()) AS waktukoreksi,
                                         DATE_FORMAT(NOW(), '%H:%i:%s') AS jamkoreksi,
                                         1 AS flagverifikasi,
                                         DATE(NOW()) AS waktuverifikasi,
                                         DATE_FORMAT(NOW(), '%H:%i:%s') AS jamverifikasi,
                                         1 AS flagpublish,
                                         NOW() AS waktupublish,
                                         DATE_FORMAT(NOW(), '%H:%i:%s') AS jampublish,
                                         NULL AS latitude,
                                         NULL AS longitude,
                                         NULL AS latitudebulanlalu,
                                         NULL AS longitudebulanlalu,
                                         1 AS adafotometer,
                                         0 AS adafotorumah,
                                         0 AS adavideo,
                                         0 AS flagminimumpakai,
                                         0 AS pakaibulanlalu,
                                         0 AS pakai2bulanlalu,
                                         0 AS pakai3bulanlalu,
                                         0 AS pakai4bulanlalu,
                                         0 AS persentasebulanlalu,
                                         0 AS persentase2bulanlalu,
                                         0 AS persentase3bulanlalu,
                                         NULL AS kelainanbulanlalu,
                                         NULL AS kelainan2bulanlalu,
                                         rek.flagangsur AS flagangsur,
                                         NULL AS idangsuran,
                                         NULL AS idmodule,
                                         0 AS flagkoreksibilling,
                                         rek.tglmulaidenda AS tglmulaidenda1,
                                         rek.tglmulaidenda2 AS tglmulaidenda2,
                                         rek.tglmulaidenda3 AS tglmulaidenda3,
                                         rek.tglmulaidenda4 AS tglmulaidenda4,
                                         rek.tglmulaidendaperbulan AS tglmulaidendaperbulan,
                                         0 AS flaghasbeenpublish,
                                         0 AS flagdrdsusulan,
                                         NULL AS waktudrdsusulan,
                                         NOW() AS waktuupdate,
                                         0 AS flaghapus
                                        FROM
                                         [bayar] rek
                                         JOIN [bsbs].pelanggan pel ON pel.nosamb = rek.nosamb
                                         JOIN temp_dataawal_periode per ON per.periode = rek.periode
                                         LEFT JOIN [bsbs].golongan gol ON gol.kodegol = rek.kodegol AND gol.aktif = 1
                                         LEFT JOIN [bsbs].diameter dia ON dia.kodediameter = rek.kodediameter AND dia.aktif = 1
                                         LEFT JOIN [bsbs].rayon ray ON ray.koderayon = rek.koderayon
                                         LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan = pel.kodekelurahan
                                         LEFT JOIN [bsbs].kolektif kol ON kol.kodekolektif = rek.kodekolektif
                                         LEFT JOIN [bsbs].byadministrasi_lain adm ON adm.kode = rek.kodeadministrasilain
                                         LEFT JOIN [bsbs].bypemeliharaan_lain pem ON pem.kode = rek.kodepemeliharaanlain
                                         LEFT JOIN [bsbs].byretribusi_lain ret ON ret.kode = rek.koderetribusilain
                                         LEFT JOIN [bsbs].pembacameter pbc ON pbc.nama = TRIM(SUBSTRING_INDEX(rek.pembacameter, '(', 1))
                                         LEFT JOIN [bsbs].kelainan kln ON kln.kelainan = rek.kelainan
                                         ,(SELECT @id := @lastid) AS id
                                         WHERE rek.periode = @periode AND rek.kode = CONCAT(rek.periode, '.', rek.nosamb) AND rek.flagangsur = 0 AND rek.flaglunas = 1 AND rek.flagbatal = 0",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam },
                                            { "@lastid", lastId },
                                            { "@periode", periode },
                                        },
                                        placeholders: new()
                                        {
                                            { "[bayar]", $"bayar{bayar}" },
                                            { "[bsbs]", AppSettings.DBNameBilling },
                                        });
                                }, usingStopwatch: true);

                                await Utils.TrackProgress($"bayar{bayar}-{periode}|rekening_air_detail", async () =>
                                {
                                    await Utils.BulkCopyV2(
                                        sConnectionStr: AppSettings.ConnectionStringLoket,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "rekening_air_detail",
                                        query: @"
                                        DROP TEMPORARY TABLE IF EXISTS temp_dataawal_periode;
                                        CREATE TEMPORARY TABLE temp_dataawal_periode (
                                            idperiode INT,
                                            periode VARCHAR(10),
                                            INDEX idx_temp_dataawal_periode_periode (periode)
                                        );
                                        INSERT INTO temp_dataawal_periode
                                        SELECT
                                        @idperiode := @idperiode+1 AS idperiode,
                                        periode
                                        FROM [bsbs].periode
                                        ,(SELECT @idperiode := 0) AS idperiode
                                        ORDER BY periode;

                                        SELECT
                                         @idpdam,
                                         pel.id AS idpelangganair,
                                         per.idperiode AS idperiode,
                                         IFNULL(rek.blok1, 0) AS blok1,
                                         IFNULL(rek.blok2, 0) AS blok2,
                                         IFNULL(rek.blok3, 0) AS blok3,
                                         IFNULL(rek.blok4, 0) AS blok4,
                                         IFNULL(rek.blok5, 0) AS blok5,
                                         IFNULL(rek.prog1, 0) AS prog1,
                                         IFNULL(rek.prog2, 0) AS prog2,
                                         IFNULL(rek.prog3, 0) AS prog3,
                                         IFNULL(rek.prog4, 0) AS prog4,
                                         IFNULL(rek.prog5, 0) AS prog5
                                        FROM
                                         [bayar] rek
                                         JOIN [bsbs].pelanggan pel ON pel.nosamb = rek.nosamb
                                         JOIN temp_dataawal_periode per ON per.periode = rek.periode
                                         WHERE rek.periode = @periode AND rek.kode = CONCAT(rek.periode, '.', rek.nosamb) AND rek.flagangsur = 0 AND rek.flaglunas = 1 AND rek.flagbatal = 0",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam },
                                            { "@periode", periode },
                                        },
                                        placeholders: new()
                                        {
                                            { "[bayar]", $"bayar{bayar}" },
                                            { "[bsbs]", AppSettings.DBNameBilling },
                                        });
                                }, usingStopwatch: true);

                                await Utils.TrackProgress($"bayar{bayar}-{periode}|rekening_air_transaksi", async () =>
                                {
                                    await Utils.BulkCopyV2(
                                        sConnectionStr: AppSettings.ConnectionStringLoket,
                                        tConnectionStr: AppSettings.ConnectionString,
                                        tableName: "rekening_air_transaksi",
                                        query: @"
                                        DROP TEMPORARY TABLE IF EXISTS temp_dataawal_periode;
                                        CREATE TEMPORARY TABLE temp_dataawal_periode (
                                            idperiode INT,
                                            periode VARCHAR(10),
                                            INDEX idx_temp_dataawal_periode_periode (periode)
                                        );
                                        INSERT INTO temp_dataawal_periode
                                        SELECT
                                        @idperiode := @idperiode+1 AS idperiode,
                                        periode
                                        FROM [bsbs].periode
                                        ,(SELECT @idperiode := 0) AS idperiode
                                        ORDER BY periode;

                                        DROP TEMPORARY TABLE IF EXISTS temp_dataawal_userloket;
                                        CREATE TEMPORARY TABLE temp_dataawal_userloket (
                                            iduser INT,
                                            nama VARCHAR(30),
                                            INDEX idx_temp_dataawal_userloket_nama (nama)
                                        );
                                        INSERT INTO temp_dataawal_userloket
                                        SELECT
                                        @iduser := @iduser + 1 AS iduser,
                                        nama
                                        FROM userloket
                                        ,(SELECT @iduser := 0) AS iduser
                                        ORDER BY nama;

                                        DROP TEMPORARY TABLE IF EXISTS temp_dataawal_loket;
                                        CREATE TEMPORARY TABLE temp_dataawal_loket (
                                            idloket INT,
                                            kodeloket VARCHAR(50),
                                            loket VARCHAR(50),
                                            INDEX idx_temp_dataawal_loket_loket (loket)
                                        );
                                        INSERT INTO temp_dataawal_loket
                                        SELECT
                                        @idloket := @idloket + 1 AS idloket,
                                        kodeloket,
                                        loket
                                        FROM loket
                                        ,(SELECT @idloket := 0) AS idloket
                                        ORDER BY kodeloket;

                                        SELECT
                                         @idpdam,
                                         pel.id AS idpelangganair,
                                         per.idperiode AS idperiode,
                                         rek.nolpp AS nomortransaksi,
                                         1 AS statustransaksi,
                                         rek.tglbayar AS waktutransaksi,
                                         YEAR(rek.tglbayar) AS tahuntransaksi,
                                         usr.iduser AS iduser,
                                         lo.idloket AS idloket,
                                         NULL AS idkolektiftransaksi,
                                         NULL AS idalasanbatal,
                                         NULL AS keterangan,
                                         NOW() AS waktuupdate
                                        FROM
                                         [bayar] rek
                                         LEFT JOIN [bsbs].pelanggan pel ON pel.nosamb = rek.nosamb
                                         LEFT JOIN temp_dataawal_periode per ON per.periode = rek.periode
                                         LEFT JOIN temp_dataawal_userloket usr ON usr.nama = rek.kasir
                                         LEFT JOIN temp_dataawal_loket lo ON lo.kodeloket = rek.loketbayar
                                         WHERE rek.periode = @periode AND rek.kode = CONCAT(rek.periode, '.', rek.nosamb) AND rek.flagangsur = 0 AND rek.flaglunas = 1 AND rek.flagbatal = 0",
                                        parameters: new()
                                        {
                                            { "@idpdam", settings.IdPdam },
                                            { "@periode", periode },
                                        },
                                        placeholders: new()
                                        {
                                            { "[bayar]", $"bayar{bayar}" },
                                            { "[bsbs]", AppSettings.DBNameBilling },
                                        });
                                }, usingStopwatch: true);
                            }

                        }
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
