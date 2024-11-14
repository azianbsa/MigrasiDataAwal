using DotNetEnv;
using Migrasi.Commands;
using Spectre.Console;
using Spectre.Console.Cli;
using System.Configuration;
using System.Diagnostics;
using Environment = Migrasi.Enums.Environment;

namespace Migrasi
{
    internal class Program
    {
        static async Task Main1(string[] args)
        {
            DataAwalConfiguration cfg = new(
                bsbsConnectionString: ConfigurationManager.AppSettings["bsbsConnectionString"]!,
                v6ConnectionString: ConfigurationManager.AppSettings["v6ConnectionString"]!);
            int _idpdam = -999;
            List<DataAwal> processList =
            [
                new(
                    processName: "Flag",
                    tableName: "master_attribute_flag",
                    queryPath: @"Queries\master_attribute_flag.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Status",
                    tableName: "master_attribute_status",
                    queryPath: @"Queries\master_attribute_status.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Jenis Bangunan",
                    tableName: "master_attribute_jenis_bangunan",
                    queryPath: @"Queries\master_attribute_jenis_bangunan.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Kepemilikan",
                    tableName: "master_attribute_kepemilikan",
                    queryPath: @"Queries\master_attribute_kepemilikan.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Pekerjaan",
                    tableName: "master_attribute_pekerjaan",
                    queryPath: @"Queries\master_attribute_pekerjaan.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Peruntukan",
                    tableName: "master_attribute_peruntukan",
                    queryPath: @"Queries\master_attribute_peruntukan.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Jenis Pipa",
                    tableName: "master_attribute_jenis_pipa",
                    queryPath: @"Queries\master_attribute_jenis_pipa.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Kwh",
                    tableName: "master_attribute_kwh",
                    queryPath: @"Queries\master_attribute_kwh.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Golongan",
                    tableName: "master_tarif_golongan",
                    queryPath: @"Queries\master_tarif_golongan.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Golongan Detail",
                    tableName: "master_tarif_golongan_detail",
                    queryPath: @"Queries\master_tarif_golongan_detail.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Diameter",
                    tableName: "master_tarif_diameter",
                    queryPath: @"Queries\master_tarif_diameter.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Diameter Detail",
                    tableName: "master_tarif_diameter_detail",
                    queryPath: @"Queries\master_tarif_diameter_detail.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Wilayah",
                    tableName: "master_attribute_wilayah",
                    queryPath: @"Queries\master_attribute_wilayah.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Area",
                    tableName: "master_attribute_area",
                    queryPath: @"Queries\master_attribute_area.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Rayon",
                    tableName: "master_attribute_rayon",
                    queryPath: @"Queries\master_attribute_rayon.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Blok",
                    tableName: "master_attribute_blok",
                    queryPath: @"Queries\master_attribute_blok.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Cabang",
                    tableName: "master_attribute_cabang",
                    queryPath: @"Queries\master_attribute_cabang.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Kecamatan",
                    tableName: "master_attribute_kecamatan",
                    queryPath: @"Queries\master_attribute_kecamatan.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Kelurahan",
                    tableName: "master_attribute_kelurahan",
                    queryPath: @"Queries\master_attribute_kelurahan.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Dma",
                    tableName: "master_attribute_dma",
                    queryPath: @"Queries\master_attribute_dma.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Dmz",
                    tableName: "master_attribute_dmz",
                    queryPath: @"Queries\master_attribute_dmz.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Administrasi Lain",
                    tableName: "master_tarif_administrasi_lain",
                    queryPath: @"Queries\master_tarif_administrasi_lain.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Pemeliharaan Lain",
                    tableName: "master_tarif_pemeliharaan_lain",
                    queryPath: @"Queries\master_tarif_pemeliharaan_lain.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Retribusi Lain",
                    tableName: "master_tarif_retribusi_lain",
                    queryPath: @"Queries\master_tarif_retribusi_lain.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Kolektif",
                    tableName: "master_attribute_kolektif",
                    queryPath: @"Queries\master_attribute_kolektif.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Sumber Air",
                    tableName: "master_attribute_sumber_air",
                    queryPath: @"Queries\master_attribute_sumber_air.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Merek Meter",
                    tableName: "master_attribute_merek_meter",
                    queryPath: @"Queries\master_attribute_merek_meter.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Kondisi Meter",
                    tableName: "master_attribute_kondisi_meter",
                    queryPath: @"Queries\master_attribute_kondisi_meter.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Kelainan",
                    tableName: "master_attribute_kelainan",
                    queryPath: @"Queries\master_attribute_kelainan.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Petugas Baca",
                    tableName: "master_attribute_petugas_baca",
                    queryPath: @"Queries\master_attribute_petugas_baca.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Periode",
                    tableName: "master_periode",
                    queryPath: @"Queries\master_periode.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: "Periode Billing",
                    tableName: "master_periode_billing",
                    queryPath: @"Queries\master_periode_billing.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName:"Pelanggan Air",
                    tableName: "master_pelanggan_air",
                    queryPath: @"Queries\master_pelanggan_air.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                        { "@lastId", 0 }
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName:"Pelanggan Air Detail",
                    tableName: "master_pelanggan_air_detail",
                    queryPath: @"Queries\master_pelanggan_air_detail.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                        { "@lastId", 0 }
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: $"DRD",
                    tableName: "rekening_air",
                    queryPath: @"Queries\drd.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                        { "@lastId", 0 }
                    },
                    placeholder: new()
                    {
                        { "[tahunbulan]", "" }
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: $"DRD Detail",
                    tableName: "rekening_air_detail",
                    queryPath: @"Queries\drd_detail.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                        { "@lastId", 0 }
                    },
                    placeholder: new()
                    {
                        { "[tahunbulan]", "" }
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: $"Piutang",
                    tableName: "rekening_air",
                    queryPath: @"Queries\piutang.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                        { "@lastId", 0 }
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: $"Piutang Detail",
                    tableName: "rekening_air_detail",
                    queryPath: @"Queries\piutang_detail.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                        { "@lastId", 0 }
                    },
                    sourceConnection: SourceConnection.Bsbs,
                    configuration: cfg),
                new(
                    processName: $"Jenis Nonair",
                    tableName: "master_attribute_jenis_nonair",
                    queryPath: @"Queries\master_attribute_jenis_nonair.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                        { "@idpdamcopy", 0 }
                    },
                    sourceConnection: null,
                    configuration: cfg),
                new(
                    processName: $"Jenis Nonair Detail",
                    tableName: "master_attribute_jenis_nonair_detail",
                    queryPath: @"Queries\master_attribute_jenis_nonair_detail.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                        { "@idpdamcopy", 0 }
                    },
                    sourceConnection: null,
                    configuration: cfg),
                new(
                    processName: $"Label Report",
                    tableName: "master_attribute_label_report",
                    queryPath: @"Queries\master_attribute_label_report.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                        { "@idpdamcopy", 0 }
                    },
                    sourceConnection: null,
                    configuration: cfg),
                new(
                    processName: $"Loket",
                    tableName: "master_attribute_loket",
                    queryPath: @"Queries\master_attribute_loket.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                        { "@idpdamcopy", 0 }
                    },
                    sourceConnection: SourceConnection.Loket,
                    configuration: cfg),
                new(
                    processName: $"Tipe Permohonan",
                    tableName: "master_attribute_tipe_permohonan",
                    queryPath: @"Queries\master_attribute_tipe_permohonan.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                        { "@idpdamcopy", 0 }
                    },
                    sourceConnection: null,
                    configuration: cfg),
                new(
                    processName: $"Tipe Permohonan Detail",
                    tableName: "master_attribute_tipe_permohonan_detail",
                    queryPath: @"Queries\master_attribute_tipe_permohonan_detail.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                        { "@idpdamcopy", 0 }
                    },
                    sourceConnection: null,
                    configuration: cfg),
                new(
                    processName: $"Tipe Permohonan Detail Ba",
                    tableName: "master_attribute_tipe_permohonan_detail_ba",
                    queryPath: @"Queries\master_attribute_tipe_permohonan_detail_ba.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                        { "@idpdamcopy", 0 }
                    },
                    sourceConnection: null,
                    configuration: cfg),
                new(
                    processName: $"Tipe Permohonan Detail Spk",
                    tableName: "master_attribute_tipe_permohonan_detail_spk",
                    queryPath: @"Queries\master_attribute_tipe_permohonan_detail_spk.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                        { "@idpdamcopy", 0 }
                    },
                    sourceConnection: null,
                    configuration: cfg),
                new(
                    processName: $"Maingroup",
                    tableName: "master_report_maingroup",
                    queryPath: @"Queries\master_report_maingroup.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                        { "@idpdamcopy", 0 }
                    },
                    sourceConnection: null,
                    configuration: cfg),
            ];
            List<string> _paketBacameter =
            [
                "flag",
                "status",
                "jenis_bangunan",
                "kepemilikan",
                "pekerjaan",
                "peruntukan",
                "jenis_pipa",
                "kwh",
                "golongan",
                "golongan_detail",
                "diameter",
                "diameter_detail",
                "wilayah",
                "area",
                "rayon",
                "blok",
                "cabang",
                "kecamatan",
                "kelurahan",
                "dma",
                "dmz",
                "administrasi_lain",
                "pemeliharaan_lain",
                "retribusi_lain",
                "kolektif",
                "sumber_air",
                "merek_meter",
                "kondisi_meter",
                "kelainan",
                "petugas_baca",
                "periode",
                "periode_billing",
                "pelanggan_air",
                "pelanggan_air_detail",
                "drd",
            ];
            List<string> _paketBasic =
            [
                "jenis_nonair",
                "jenis_nonair_detail",
                "label_report",
                "loket",
                "tipe_permohonan",
                "tipe_permohonan_detail",
                "tipe_permohonan_detail_ba",
                "tipe_permohonan_detail_spk",
                "maingroup",
            ];

            var arguments = ParseArguments(args);

            PrintConnection();

            Console.Write("Apakah anda yakin ingin melanjutkan? (y/n): ");
            var answer = Console.ReadKey();
            if (answer.KeyChar == 'n')
            {
                return;
            }
            Console.WriteLine();

            #region options

            #region idpdam
            if (arguments.TryGetValue("--idpdam", out string? idpdam))
            {
                _idpdam = !string.IsNullOrWhiteSpace(idpdam) ? int.Parse(idpdam) : -999;
            }
            #endregion

            #region piutang

            if (arguments.TryGetValue("-p", out string? _))
            {
                await Piutang(_idpdam, processList);
                return;
            }

            if (arguments.TryGetValue("--piutang", out string? _))
            {
                await Piutang(_idpdam, processList);
                return;
            }

            #endregion

            #region paket
            if (arguments.TryGetValue("--paket", out string? paket) && arguments.TryGetValue("--batas-bawah-periode", out string? batasBawahPeriode))
            {
                if (string.IsNullOrWhiteSpace(paket)) return;
                if (string.IsNullOrWhiteSpace(batasBawahPeriode)) return;

                int _batasBawahPeriode = int.Parse(batasBawahPeriode);

                try
                {
                    switch (paket)
                    {
                        case "bacameter":
                            {
                                #region data pelanggan cleanup
                                await new DataAwal(
                                    processName: "Bsbs Pelanggan Cleanup",
                                    tableName: "",
                                    queryPath: @"Queries\Patches\tambah_field_id_tabel_pelanggan.sql",
                                    sourceConnection: null,
                                    configuration: cfg).ExecuteAsync1();
                                await new DataAwal(
                                    processName: "Bsbs Pelanggan Cleanup Golongan",
                                    tableName: "",
                                    queryPath: @"Queries\Patches\data_cleanup_golongan.sql",
                                    placeholder: new()
                                    {
                                        { "[table]", "pelanggan" }
                                    },
                                    sourceConnection: null,
                                    configuration: cfg).ExecuteAsync1();
                                await new DataAwal(
                                    processName: "Bsbs Pelanggan Cleanup Diameter",
                                    tableName: "",
                                    queryPath: @"Queries\Patches\data_cleanup_diameter.sql",
                                    parameter: [],
                                    placeholder: new()
                                    {
                                        { "[table]", "pelanggan" }
                                    },
                                    sourceConnection: null,
                                    configuration: cfg).ExecuteAsync1();
                                await new DataAwal(
                                    processName: "Bsbs Pelanggan Cleanup Merek Meter",
                                    tableName: "",
                                    queryPath: @"Queries\Patches\data_cleanup_merek_meter.sql",
                                    sourceConnection: null,
                                    configuration: cfg).ExecuteAsync1();
                                await new DataAwal(
                                    processName: "Bsbs Pelanggan Cleanup Kelurahan",
                                    tableName: "",
                                    queryPath: @"Queries\Patches\data_cleanup_kelurahan.sql",
                                    placeholder: new()
                                    {
                                        { "[table]", "pelanggan" }
                                    },
                                    sourceConnection: null,
                                    configuration: cfg).ExecuteAsync1();
                                await new DataAwal(
                                    processName: "Bsbs Pelanggan Cleanup Kolektif",
                                    tableName: "",
                                    queryPath: @"Queries\Patches\data_cleanup_kolektif.sql",
                                    placeholder: new()
                                    {
                                        { "[table]", "pelanggan" }
                                    },
                                    sourceConnection: null,
                                    configuration: cfg).ExecuteAsync1();
                                await new DataAwal(
                                    processName: "Bsbs Pelanggan Cleanup Sumber Air",
                                    tableName: "",
                                    queryPath: @"Queries\Patches\data_cleanup_sumber_air.sql",
                                    sourceConnection: null,
                                    configuration: cfg).ExecuteAsync1();
                                await new DataAwal(
                                    processName: "Bsbs Pelanggan Cleanup Blok",
                                    tableName: "",
                                    queryPath: @"Queries\Patches\data_cleanup_blok.sql",
                                    sourceConnection: null,
                                    configuration: cfg).ExecuteAsync1();
                                await new DataAwal(
                                    processName: "Bsbs Pelanggan Cleanup Kondisi Meter",
                                    tableName: "",
                                    queryPath: @"Queries\Patches\data_cleanup_kondisi_meter.sql",
                                    sourceConnection: null,
                                    configuration: cfg).ExecuteAsync1();
                                await new DataAwal(
                                    processName: "Bsbs Pelanggan Cleanup Adm. Lain",
                                    tableName: "",
                                    queryPath: @"Queries\Patches\data_cleanup_adm_lain.sql",
                                    placeholder: new()
                                    {
                                        { "[table]", "pelanggan" }
                                    },
                                    sourceConnection: null,
                                    configuration: cfg).ExecuteAsync1();
                                await new DataAwal(
                                    processName: "Bsbs Pelanggan Cleanup Pem. Lain",
                                    tableName: "",
                                    queryPath: @"Queries\Patches\data_cleanup_pem_lain.sql",
                                    placeholder: new()
                                    {
                                        { "[table]", "pelanggan" }
                                    },
                                    sourceConnection: null,
                                    configuration: cfg).ExecuteAsync1();
                                await new DataAwal(
                                    processName: "Bsbs Pelanggan Cleanup Ret. Lain",
                                    tableName: "",
                                    queryPath: @"Queries\Patches\data_cleanup_ret_lain.sql",
                                    placeholder: new()
                                    {
                                        { "[table]", "pelanggan" }
                                    },
                                    sourceConnection: null,
                                    configuration: cfg).ExecuteAsync1();

                                #endregion

                                foreach (var key in _paketBacameter)
                                {
                                    var process = processList.Where(s => s.Key == key).First();

                                    process.Parameter!["@idpdam"] = _idpdam;

                                    if (key == "drd")
                                    {
                                        var processDetail = processList.Where(s => s.Key == "drd_detail").First();
                                        processDetail.Parameter!["@idpdam"] = _idpdam;

                                        for (var i = _batasBawahPeriode; i < _batasBawahPeriode + 4; i++)
                                        {
                                            #region data drd cleanup

                                            await new DataAwal(
                                                processName: $"Bsbs DRD{i} Cleanup Golongan",
                                                tableName: "",
                                                queryPath: @"Queries\Patches\data_cleanup_golongan.sql",
                                                placeholder: new()
                                                {
                                                    { "[table]", $"drd{i}" }
                                                },
                                                sourceConnection: null,
                                                configuration: cfg).ExecuteAsync1();
                                            await new DataAwal(
                                                processName: $"Bsbs DRD{i} Cleanup Diameter",
                                                tableName: "",
                                                queryPath: @"Queries\Patches\data_cleanup_diameter.sql",
                                                parameter: [],
                                                placeholder: new()
                                                {
                                                    { "[table]", $"drd{i}" }
                                                },
                                                sourceConnection: null,
                                                configuration: cfg).ExecuteAsync1();
                                            await new DataAwal(
                                                processName: $"Bsbs DRD{i} Cleanup Kelurahan",
                                                tableName: "",
                                                queryPath: @"Queries\Patches\data_cleanup_kelurahan.sql",
                                                placeholder: new()
                                                {
                                                    { "[table]", $"drd{i}" }
                                                },
                                                sourceConnection: null,
                                                configuration: cfg).ExecuteAsync1();
                                            await new DataAwal(
                                                processName: $"Bsbs DRD{i} Cleanup Kolektif",
                                                tableName: "",
                                                queryPath: @"Queries\Patches\data_cleanup_kolektif.sql",
                                                placeholder: new()
                                                {
                                                    { "[table]", $"drd{i}" }
                                                },
                                                sourceConnection: null,
                                                configuration: cfg).ExecuteAsync1();
                                            await new DataAwal(
                                                processName: $"Bsbs DRD{i} Cleanup Adm. Lain",
                                                tableName: "",
                                                queryPath: @"Queries\Patches\data_cleanup_adm_lain.sql",
                                                placeholder: new()
                                                {
                                                    { "[table]", $"drd{i}" }
                                                },
                                                sourceConnection: null,
                                                configuration: cfg).ExecuteAsync1();
                                            await new DataAwal(
                                                processName: $"Bsbs DRD{i} Cleanup Pem. Lain",
                                                tableName: "",
                                                queryPath: @"Queries\Patches\data_cleanup_pem_lain.sql",
                                                placeholder: new()
                                                {
                                                    { "[table]", $"drd{i}" }
                                                },
                                                sourceConnection: null,
                                                configuration: cfg).ExecuteAsync1();
                                            await new DataAwal(
                                                processName: $"Bsbs DRD{i} Cleanup Ret. Lain",
                                                tableName: "",
                                                queryPath: @"Queries\Patches\data_cleanup_ret_lain.sql",
                                                placeholder: new()
                                                {
                                                    { "[table]", $"drd{i}" }
                                                },
                                                sourceConnection: null,
                                                configuration: cfg).ExecuteAsync1();

                                            #endregion

                                            process.ProcessName = $"DRD{i}";
                                            process.Placeholder!["[tahunbulan]"] = i.ToString();
                                            await process.ProsesAsync();

                                            processDetail.ProcessName = $"DRD{i} Detail";
                                            processDetail.Placeholder!["[tahunbulan]"] = i.ToString();
                                            await processDetail.ProsesAsync();
                                        }
                                    }
                                    else
                                    {
                                        await process.ProsesAsync();
                                    }
                                }

                                break;
                            }
                        case "basic":
                            {

                                break;
                            }
                        default:
                            break;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss zzz}] error: {e.Message}");
                }

                return;
            }
            #endregion

            #endregion

            #region commands

            #region help
            if (arguments.ContainsKey("help") || arguments.Count == 0)
            {
                CommandHelp();
                return;
            }
            #endregion

            #region ls
            if (arguments.TryGetValue("ls", out string? _))
            {
                CommandLs(processList);
                return;
            }
            #endregion

            #region new
            if (arguments.TryGetValue("new", out string? _) && arguments.TryGetValue("--nama-pdam", out string? namaPdam) && arguments.TryGetValue("--copy-dari-idpdam", out string? idPdamCopy))
            {
                await CommandNew(cfg, _idpdam, namaPdam, idPdamCopy);
            }
            #endregion

            #endregion
        }

        private static void PrintConnection()
        {
            Console.WriteLine("Connection list:");
            Console.WriteLine($"bsbs        : {ConfigurationManager.AppSettings["bsbsConnectionString"]!}");
            Console.WriteLine($"loket       : {ConfigurationManager.AppSettings["loketConnectionString"]!}");
            Console.WriteLine($"bacameter   : {ConfigurationManager.AppSettings["bacameterConnectionString"]!}");
            Console.WriteLine($"v6          : {ConfigurationManager.AppSettings["v6ConnectionString"]!}");
            Console.WriteLine();
        }

        private static async Task CommandNew(DataAwalConfiguration configuration, int idPdam, string? namaPdam, string? idPdamCopy)
        {
            try
            {
                await new DataAwal(
                    processName: "Setup New PDAM",
                    tableName: "",
                    queryPath: @"Queries\Patches\setup_new_pdam.sql",
                    parameter: new()
                    {
                        { "@idpdam", idPdam },
                        { "@namapdam", namaPdam },
                        { "@idpdamcopy", int.Parse(idPdamCopy ?? "0") },
                    },
                    sourceConnection: null,
                    configuration: configuration).ExecuteAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine($"error: {e.Message}");
            }
        }

        private static async Task Piutang(int idPdam, List<DataAwal> processList)
        {
            try
            {
                var piutang = processList.Where(s => s.Key == "piutang").First();
                piutang.Parameter!["@idpdam"] = idPdam;
                await piutang.ProsesAsync();

                var piutangDetail = processList.Where(s => s.Key == "piutang_detail").First();
                piutangDetail.Parameter!["@idpdam"] = idPdam;
                await piutangDetail.ProsesAsync();
            }
            catch (Exception)
            {
            }
        }

        private static void CommandLs(List<DataAwal> processList)
        {
            Console.WriteLine("Process List:");
            Console.WriteLine();
            foreach (var process in processList)
            {
                Console.WriteLine(process.ToString());
            }
        }

        private static void CommandHelp()
        {
            Console.WriteLine("Usage: Migrasi [OPTIONS] COMMAND");
            Console.WriteLine();
            Console.WriteLine("Tools migrasi data dari v4 ke v6");
            Console.WriteLine();
            Console.WriteLine("Commands:");
            Console.WriteLine("  ls         Print proses yang bisa dijalankan");
            Console.WriteLine("  new        Setup pdam baru");
            Console.WriteLine();
            Console.WriteLine("Options:");
            Console.WriteLine("  --idpdam  int                      ID PDAM (default -999)");
            Console.WriteLine("  --paket  string                    Pilih paket (bacameter|basic)");
            Console.WriteLine("  --batas-bawah-periode  int         Drd{tahunbulan} (wajib diisi untuk paket bacameter)");
            Console.WriteLine("  -p, --piutang                      Migrasi piutang");
            Console.WriteLine("  --nama-pdam string                 Nama pdam untuk pdam baru");
            Console.WriteLine("  --copy-dari-idpdam int             Copy data dari idpdam");
        }

        private static Dictionary<string, string?> ParseArguments(string[] args)
        {
            var arguments = new Dictionary<string, string?>();

            foreach (var arg in args)
            {
                // Split the argument by '=' to handle key/value pairs
                string[] parts = arg.Split('=');

                // Check if the argument is in the format "key=value"
                if (parts.Length == 2)
                {
                    arguments[parts[0]] = parts[1];
                }
                // If not, assume it's just a named argument without a value
                else
                {
                    arguments[arg] = null;
                }
            }

            return arguments;
        }

        public static int Main(string[] args)
        {
            #region environment variables

            Env.Load(".env");

            AppSettings.Environment = (Environment)Enum.Parse(typeof(Environment), Env.GetString("ENVIRONMENT", "Development"));

            var dbSuffix = (Environment)Enum.Parse(typeof(Environment), Env.GetString("ENVIRONMENT", "Development")) switch
            {
                Environment.Development => "_DEV",
                Environment.Staging => "_STG",
                Environment.Production => "_PRD",
                _ => "_DEV"
            };

            AppSettings.DBHost = Env.GetString($"DB_HOST{dbSuffix}");
            AppSettings.DBPort = (uint)Env.GetInt($"DB_PORT{dbSuffix}");
            AppSettings.DBUser = Env.GetString($"DB_USER{dbSuffix}");
            AppSettings.DBPassword = Env.GetString($"DB_PASSWORD{dbSuffix}");
            AppSettings.DBName = Env.GetString($"DB_NAME{dbSuffix}");

            AppSettings.DBHostStaging = Env.GetString($"DB_HOST_STG");
            AppSettings.DBPortStaging = (uint)Env.GetInt($"DB_PORT_STG");
            AppSettings.DBUserStaging = Env.GetString($"DB_USER_STG");
            AppSettings.DBPasswordStaging = Env.GetString($"DB_PASSWORD_STG");
            AppSettings.DBNameStaging = Env.GetString($"DB_NAME_STG");

            AppSettings.DBHostBilling = Env.GetString($"DB_HOST_BILLING");
            AppSettings.DBPortBilling = (uint)Env.GetInt($"DB_PORT_BILLING");
            AppSettings.DBUserBilling = Env.GetString($"DB_USER_BILLING");
            AppSettings.DBPasswordBilling = Env.GetString($"DB_PASSWORD_BILLING");
            AppSettings.DBNameBilling = Env.GetString($"DB_NAME_BILLING");

            AppSettings.DBHostBacameter = Env.GetString($"DB_HOST_BACAMETER");
            AppSettings.DBPortBacameter = (uint)Env.GetInt($"DB_PORT_BACAMETER");
            AppSettings.DBUserBacameter = Env.GetString($"DB_USER_BACAMETER");
            AppSettings.DBPasswordBacameter = Env.GetString($"DB_PASSWORD_BACAMETER");
            AppSettings.DBNameBacameter = Env.GetString($"DB_NAME_BACAMETER");
            
            AppSettings.DBHostLoket = Env.GetString($"DB_HOST_LOKET");
            AppSettings.DBPortLoket = (uint)Env.GetInt($"DB_PORT_LOKET");
            AppSettings.DBUserLoket = Env.GetString($"DB_USER_LOKET");
            AppSettings.DBPasswordLoket = Env.GetString($"DB_PASSWORD_LOKET");
            AppSettings.DBNameLoket = Env.GetString($"DB_NAME_LOKET");

            #endregion

            var app = new CommandApp();

            app.Configure(config =>
            {
                config.PropagateExceptions();
                config.AddCommand<NewCommand>("new");
                config.AddCommand<PaketCommand>("paket");
                //config.AddCommand<PiutangCommand>("piutang");
                //config.AddCommand<BayarCommand>("bayar");
                //config.AddCommand<NonairCommand>("nonair");
                //config.AddCommand<PermohonanCommand>("permohonan");
            });

            var sw = Stopwatch.StartNew();
            try
            {
                AnsiConsole.Write(new FigletText("Data Awal v6").Color(Color.Aqua));
                return app.Run(args);
            }
            catch (Exception ex)
            {
                AnsiConsole.WriteException(ex, ExceptionFormats.ShortenEverything);
                return -99;
            }
            finally
            {
                sw.Stop();
                AnsiConsole.MarkupLine($"[bold green]Program exit. (elapsed {sw.Elapsed})[/]");
            }
        }
    }
}
