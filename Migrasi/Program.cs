using System.Configuration;

namespace Migrasi
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            DataAwalConfiguration cfg = new(ConfigurationManager.AppSettings["sourceConnection"]!, ConfigurationManager.AppSettings["targetConnection"]!);
            int _idpdam;
            string _paket;

            var arguments = ParseArguments(args);

            if (arguments.ContainsKey("--help") || arguments.Count == 0)
            {
                PrintHelp();
                return;
            }

            if (arguments.TryGetValue("--idpdam", out string? idpdam))
            {
                if (string.IsNullOrWhiteSpace(idpdam)) return;
                _idpdam = int.Parse(idpdam);
                Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss zzz}] idpdam: {_idpdam}");
            }
            else
            {
                throw new ArgumentNullException(paramName: nameof(_idpdam), message: "option --idpdam is required.");
            }

            if (arguments.TryGetValue("--paket", out string? paket) && arguments.TryGetValue("--batas-bawah-periode", out string? batasBawahPeriode))
            {
                if (string.IsNullOrWhiteSpace(paket)) return;
                if (string.IsNullOrWhiteSpace(batasBawahPeriode)) return;
                
                _paket = paket;
                int _batasBawahPeriode = int.Parse(batasBawahPeriode);
                
                Console.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss zzz}] {nameof(paket)}: {_paket}");

                try
                {
                    switch (paket)
                    {
                        case "bacameter":
                            {
                                await new DataAwal(
                                    processName: "Flag",
                                    tableName: "master_attribute_flag",
                                    queryPath: @"Queries\master_attribute_flag.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Status",
                                    tableName: "master_attribute_status",
                                    queryPath: @"Queries\master_attribute_status.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Jenis Bangunan",
                                    tableName: "master_attribute_jenis_bangunan",
                                    queryPath: @"Queries\master_attribute_jenis_bangunan.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Kepemilikan",
                                    tableName: "master_attribute_kepemilikan",
                                    queryPath: @"Queries\master_attribute_kepemilikan.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Pekerjaan",
                                    tableName: "master_attribute_pekerjaan",
                                    queryPath: @"Queries\master_attribute_pekerjaan.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Peruntukan",
                                    tableName: "master_attribute_peruntukan",
                                    queryPath: @"Queries\master_attribute_peruntukan.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Jenis Pipa",
                                    tableName: "master_attribute_jenis_pipa",
                                    queryPath: @"Queries\master_attribute_jenis_pipa.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Kwh",
                                    tableName: "master_attribute_kwh",
                                    queryPath: @"Queries\master_attribute_kwh.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Golongan",
                                    tableName: "master_tarif_golongan",
                                    queryPath: @"Queries\master_tarif_golongan.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Golongan Detail",
                                    tableName: "master_tarif_golongan_detail",
                                    queryPath: @"Queries\master_tarif_golongan_detail.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Diameter",
                                    tableName: "master_tarif_diameter",
                                    queryPath: @"Queries\master_tarif_diameter.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Diameter Detail",
                                    tableName: "master_tarif_diameter_detail",
                                    queryPath: @"Queries\master_tarif_diameter_detail.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Wilayah",
                                    tableName: "master_attribute_wilayah",
                                    queryPath: @"Queries\master_attribute_wilayah.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Area",
                                    tableName: "master_attribute_area",
                                    queryPath: @"Queries\master_attribute_area.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Rayon",
                                    tableName: "master_attribute_rayon",
                                    queryPath: @"Queries\master_attribute_rayon.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Blok",
                                    tableName: "master_attribute_blok",
                                    queryPath: @"Queries\master_attribute_blok.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Cabang",
                                    tableName: "master_attribute_cabang",
                                    queryPath: @"Queries\master_attribute_cabang.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Kecamatan",
                                    tableName: "master_attribute_kecamatan",
                                    queryPath: @"Queries\master_attribute_kecamatan.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Kelurahan",
                                    tableName: "master_attribute_kelurahan",
                                    queryPath: @"Queries\master_attribute_kelurahan.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Dma",
                                    tableName: "master_attribute_dma",
                                    queryPath: @"Queries\master_attribute_dma.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Dmz",
                                    tableName: "master_attribute_dmz",
                                    queryPath: @"Queries\master_attribute_dmz.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Administrasi Lain",
                                    tableName: "master_tarif_administrasi_lain",
                                    queryPath: @"Queries\master_tarif_administrasi_lain.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Pemeliharaan Lain",
                                    tableName: "master_tarif_pemeliharaan_lain",
                                    queryPath: @"Queries\master_tarif_pemeliharaan_lain.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Retribusi Lain",
                                    tableName: "master_tarif_retribusi_lain",
                                    queryPath: @"Queries\master_tarif_retribusi_lain.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Kolektif",
                                    tableName: "master_attribute_kolektif",
                                    queryPath: @"Queries\master_attribute_kolektif.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Sumber Air",
                                    tableName: "master_attribute_sumber_air",
                                    queryPath: @"Queries\master_attribute_sumber_air.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Merek Meter",
                                    tableName: "master_attribute_merek_meter",
                                    queryPath: @"Queries\master_attribute_merek_meter.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Kondisi Meter",
                                    tableName: "master_attribute_kondisi_meter",
                                    queryPath: @"Queries\master_attribute_kondisi_meter.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Kelainan",
                                    tableName: "master_attribute_kelainan",
                                    queryPath: @"Queries\master_attribute_kelainan.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Petugas Baca",
                                    tableName: "master_attribute_petugas_baca",
                                    queryPath: @"Queries\master_attribute_petugas_baca.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Periode",
                                    tableName: "master_periode",
                                    queryPath: @"Queries\master_periode.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Periode Billing",
                                    tableName: "master_periode_billing",
                                    queryPath: @"Queries\master_periode_billing.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Pelanggan Air",
                                    tableName: "master_pelanggan_air",
                                    queryPath: @"Queries\master_pelanggan_air.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    { "@lastId", 0 }
                                    },
                                    configuration: cfg).ProsesAsync();
                                await new DataAwal(
                                    processName: "Pelanggan Air Detail",
                                    tableName: "master_pelanggan_air_detail",
                                    queryPath: @"Queries\master_pelanggan_air_detail.sql",
                                    parameter: new()
                                    {
                                    { "@idpdam", _idpdam },
                                    { "@lastId", 0 }
                                    },
                                    configuration: cfg).ProsesAsync();

                                for (var i = _batasBawahPeriode; i < _batasBawahPeriode + 4; i++)
                                {
                                    await new DataAwal(
                                        processName: $"DRD{i}",
                                        tableName: "rekening_air",
                                        queryPath: @"Queries\drd.sql",
                                        drdTahunBulan: i,
                                        parameter: new()
                                        {
                                            { "@idpdam", _idpdam },
                                            { "@lastId", 0 }
                                        },
                                        configuration: cfg).ProsesAsync();
                                    await new DataAwal(
                                        processName: $"DRD{i} Detail",
                                        tableName: "rekening_air_detail",
                                        queryPath: @"Queries\drd_detail.sql",
                                        drdTahunBulan: i,
                                        parameter: new()
                                        {
                                            { "@idpdam", _idpdam },
                                            { "@lastId", 0 }
                                        },
                                        configuration: cfg).ProsesAsync();
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
            }

            if (arguments.TryGetValue("--piutang", out string? _))
            {
                try
                {
                    await new DataAwal(
                        processName: $"Piutang",
                        tableName: "rekening_air",
                        queryPath: @"Queries\piutang.sql",
                        parameter: new()
                        {
                            { "@idpdam", _idpdam },
                            { "@lastId", 0 }
                        },
                        configuration: cfg).ProsesAsync();
                    await new DataAwal(
                        processName: $"Piutang Detail",
                        tableName: "rekening_air_detail",
                        queryPath: @"Queries\piutang_detail.sql",
                        parameter: new()
                        {
                            { "@idpdam", _idpdam },
                            { "@lastId", 0 }
                        },
                        configuration: cfg).ProsesAsync();
                }
                catch (Exception)
                {
                }
            }
        }

        static Dictionary<string, string?> ParseArguments(string[] args)
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

        static void PrintHelp()
        {
            Console.WriteLine("Help:");
            Console.WriteLine("-----");
            Console.WriteLine("Usage: Migrasi [options]");
            Console.WriteLine();
            Console.WriteLine("Options:");
            Console.WriteLine("  --help                                 Display this help message");
            Console.WriteLine("  --idpdam=<idpdam>                      <idpdam>");
            Console.WriteLine("  --paket=<paket>                        <bacameter>|<basic>");
            Console.WriteLine("  --batas-bawah-periode=<tahunbulan>     Harus diisi jika paket yang dipilih adalah bacameter");
            Console.WriteLine("  --piutang                              Proses migrasi piutang");
        }
    }
}
