using System.Configuration;

namespace Migrasi
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            DataAwalConfiguration cfg = new(
                bsbsConnectionString: ConfigurationManager.AppSettings["bsbsConnectionString"]!,
                v6ConnectionString: ConfigurationManager.AppSettings["v6ConnectionString"]!);
            DataAwalConfiguration cfgCopy = new(
                bsbsConnectionString: ConfigurationManager.AppSettings["v6ConnectionString"]!,
                v6ConnectionString: ConfigurationManager.AppSettings["v6ConnectionString"]!);
            int _idpdam = -999;
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
                    queryPath: @"Queries\master_attribute_jenis_nonair_copy.sql",
                    parameter: new()
                    {
                        { "@idpdam", null },
                        { "@idpdamcopy", 0 }
                    },
                    sourceConnection: SourceConnection.V6,
                    configuration: cfgCopy),
            ];

            var arguments = ParseArguments(args);

            if (arguments.ContainsKey("help") || arguments.Count == 0)
            {
                PrintHelp();
                return;
            }

            if (arguments.TryGetValue("ls", out string? _))
            {
                PrintProcessList(processList);
                return;
            }

            if (arguments.TryGetValue("--idpdam", out string? idpdam))
            {
                _idpdam = !string.IsNullOrWhiteSpace(idpdam) ? int.Parse(idpdam) : -999;
            }

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
                                            process.ProcessName = $"DRD{i}";
                                            process.DrdTahunBulan = i;
                                            await process.ProsesAsync();

                                            processDetail.ProcessName = $"DRD{i} Detail";
                                            processDetail.DrdTahunBulan = i;
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
                catch (Exception)
                {
                }

                return;
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

        private static void PrintProcessList(List<DataAwal> processList)
        {
            Console.WriteLine("Process List:");
            Console.WriteLine();
            foreach (var process in processList)
            {
                Console.WriteLine(process.ToString());
            }
        }

        private static void PrintHelp()
        {
            Console.WriteLine("Usage: Migrasi [OPTIONS] COMMAND");
            Console.WriteLine();
            Console.WriteLine("Tools migrasi data dari v4 ke v6");
            Console.WriteLine();
            Console.WriteLine("Commands:");
            Console.WriteLine("  ls         Print proses yang bisa dijalankan");
            Console.WriteLine();
            Console.WriteLine("Options:");
            Console.WriteLine("  --idpdam  int                      ID PDAM (default -999)");
            Console.WriteLine("  --paket  string                    Pilih paket (bacameter|basic)");
            Console.WriteLine("  --batas-bawah-periode  int         Drd{tahunbulan} (wajib diisi untuk paket bacameter)");
            Console.WriteLine("  -p, --piutang                      Migrasi piutang");
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
    }
}
