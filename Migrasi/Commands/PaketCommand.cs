using Dapper;
using Migrasi.Helpers;
using MySqlConnector;
using Spectre.Console;
using Spectre.Console.Cli;
using Sprache;

namespace Migrasi.Commands
{
    public class PaketCommand : AsyncCommand<PaketCommand.Settings>
    {
        private static readonly Dictionary<string, List<MySqlBulkCopyColumnMapping>> ColumnMappings = new()
        {
            {
                "master_pelanggan_air",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpelangganair"),
                    new(2, "nosamb"),
                    new(3, "norekening"),
                    new(4, "nama"),
                    new(5, "alamat"),
                    new(6, "rt"),
                    new(7, "rw"),
                    new(8, "idgolongan"),
                    new(9, "iddiameter"),
                    new(10, "idjenispipa"),
                    new(11, "idkwh"),
                    new(12, "idrayon"),
                    new(13, "idkelurahan"),
                    new(14, "idkolektif"),
                    new(15, "idstatus"),
                    new(16, "idflag"),
                    new(17, "latitude"),
                    new(18, "longitude"),
                    new(19, "akurasi"),
                    new(20, "nosamblama"),
                    new(21, "flaghapus"),
                    new(22, "waktuupdate"),
                }
            },
            {
                "master_pelanggan_air_detail",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpelangganair"),
                    new(2, "idsumberair"),
                    new(3, "iddma"),
                    new(4, "iddmz"),
                    new(5, "idblok"),
                    new(6, "idmerekmeter"),
                    new(7, "idkondisimeter"),
                    new(8, "idadministrasilain"),
                    new(9, "idpemeliharaanlain"),
                    new(10, "idretribusilain"),
                    new(11, "idpekerjaan"),
                    new(12, "idjenisbangunan"),
                    new(13, "idperuntukan"),
                    new(14, "idkepemilikan"),
                    new(15, "nosegel"),
                    new(16, "nohp"),
                    new(17, "notelp"),
                    new(18, "noktp"),
                    new(19, "nokk"),
                    new(20, "email"),
                    new(21, "noserimeter"),
                    new(22, "tglmeter"),
                    new(23, "pekerjaan"),
                    new(24, "penghuni"),
                    new(25, "namapemilik"),
                    new(26, "alamatpemilik"),
                    new(27, "kodepost"),
                    new(28, "dayalistrik"),
                    new(29, "luastanah"),
                    new(30, "luasrumah"),
                    new(31, "urutanbaca"),
                    new(32, "stanawalpasang"),
                    new(33, "nopendaftaran"),
                    new(34, "tgldaftar"),
                    new(35, "tglpenentuanbaca"),
                    new(36, "norab"),
                    new(37, "nobapemasangan"),
                    new(38, "tglpasang"),
                    new(39, "tglputus"),
                    new(40, "noserimeterlama"),
                    new(41, "kategoriputus"),
                    new(42, "idtipependaftaransambungan"),
                    new(43, "idkategorikawasan"),
                    new(44, "keterangan"),
                    new(45, "waktuupdate"),
                }
            },
            {
                "rekening_air",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idrekeningair"),
                    new(2, "idpelangganair"),
                    new(3, "idperiode"),
                    new(4, "idgolongan"),
                    new(5, "iddiameter"),
                    new(6, "idjenispipa"),
                    new(7, "idkwh"),
                    new(8, "idrayon"),
                    new(9, "idkelurahan"),
                    new(10, "idkolektif"),
                    new(11, "idadministrasilain"),
                    new(12, "idpemeliharaanlain"),
                    new(13, "idretribusilain"),
                    new(14, "idstatus"),
                    new(15, "idflag"),
                    new(16, "stanlalu"),
                    new(17, "stanskrg"),
                    new(18, "stanangkat"),
                    new(19, "pakai"),
                    new(20, "pakaikalkulasi"),
                    new(21, "biayapemakaian"),
                    new(22, "administrasi"),
                    new(23, "pemeliharaan"),
                    new(24, "retribusi"),
                    new(25, "pelayanan"),
                    new(26, "airlimbah"),
                    new(27, "dendapakai0"),
                    new(28, "administrasilain"),
                    new(29, "pemeliharaanlain"),
                    new(30, "retribusilain"),
                    new(31, "ppn"),
                    new(32, "meterai"),
                    new(33, "rekair"),
                    new(34, "denda"),
                    new(35, "diskon"),
                    new(36, "deposit"),
                    new(37, "total"),
                    new(38, "hapussecaraakuntansi"),
                    new(39, "waktuhapussecaraakuntansi"),
                    new(40, "iddetailcyclepembacaan"),
                    new(41, "tglpenentuanbaca"),
                    new(42, "flagbaca"),
                    new(43, "metodebaca"),
                    new(44, "waktubaca"),
                    new(45, "jambaca"),
                    new(46, "petugasbaca"),
                    new(47, "kelainan"),
                    new(48, "stanbaca"),
                    new(49, "waktukirimhasilbaca"),
                    new(50, "jamkirimhasilbaca"),
                    new(51, "memolapangan"),
                    new(52, "lampiran"),
                    new(53, "taksasi"),
                    new(54, "taksir"),
                    new(55, "flagrequestbacaulang"),
                    new(56, "waktuupdaterequestbacaulang"),
                    new(57, "flagkoreksi"),
                    new(58, "waktukoreksi"),
                    new(59, "jamkoreksi"),
                    new(60, "flagverifikasi"),
                    new(61, "waktuverifikasi"),
                    new(62, "jamverifikasi"),
                    new(63, "userverifikasi"),
                    new(64, "flagpublish"),
                    new(65, "waktupublish"),
                    new(66, "jampublish"),
                    new(67, "userpublish"),
                    new(68, "latitude"),
                    new(69, "longitude"),
                    new(70, "latitudebulanlalu"),
                    new(71, "longitudebulanlalu"),
                    new(72, "adafotometer"),
                    new(73, "adafotorumah"),
                    new(74, "adavideo"),
                    new(75, "flagminimumpakai"),
                    new(76, "pakaibulanlalu"),
                    new(77, "pakai2bulanlalu"),
                    new(78, "pakai3bulanlalu"),
                    new(79, "pakai4bulanlalu"),
                    new(80, "persentasebulanlalu"),
                    new(81, "persentase2bulanlalu"),
                    new(82, "persentase3bulanlalu"),
                    new(83, "kelainanbulanlalu"),
                    new(84, "kelainan2bulanlalu"),
                    new(85, "flagangsur"),
                    new(86, "idangsuran"),
                    new(87, "idmodule"),
                    new(88, "flagkoreksibilling"),
                    new(89, "tglmulaidenda1"),
                    new(90, "tglmulaidenda2"),
                    new(91, "tglmulaidenda3"),
                    new(92, "tglmulaidenda4"),
                    new(93, "tglmulaidendaperbulan"),
                    new(94, "tglmulaidendaperhari"),
                    new(95, "flaghasbeenpublish"),
                    new(96, "flagdrdsusulan"),
                    new(97, "waktudrdsusulan"),
                    new(98, "waktuupdate"),
                    new(99, "flaghapus"),
                }
            },
            {
                "rekening_air_detail",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpelangganair"),
                    new(2, "idperiode"),
                    new(3, "blok1"),
                    new(4, "blok2"),
                    new(5, "blok3"),
                    new(6, "blok4"),
                    new(7, "blok5"),
                    new(8, "prog1"),
                    new(9, "prog2"),
                    new(10, "prog3"),
                    new(11, "prog4"),
                    new(12, "prog5"),
                }
            },
            {
                "rekening_air_transaksi",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpelangganair"),
                    new(2, "idperiode"),
                    new(3, "nomortransaksi"),
                    new(4, "statustransaksi"),
                    new(5, "waktutransaksi"),
                    new(6, "tahuntransaksi"),
                    new(7, "iduser"),
                    new(8, "idloket"),
                    new(9, "idkolektiftransaksi"),
                    new(10, "idalasanbatal"),
                    new(11, "keterangan"),
                    new(12, "waktuupdate"),
                }
            },
            {
                "rekening_nonair",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idnonair"),
                    new(2, "idjenisnonair"),
                    new(3, "idpelangganair"),
                    new(4, "idpelangganlimbah"),
                    new(5, "idpelangganlltt"),
                    new(6, "kodeperiode"),
                    new(7, "nomornonair"),
                    new(8, "keterangan"),
                    new(9, "total"),
                    new(10, "tanggalmulaitagih"),
                    new(11, "tanggalkadaluarsa"),
                    new(12, "nama"),
                    new(13, "alamat"),
                    new(14, "idrayon"),
                    new(15, "idkelurahan"),
                    new(16, "idgolongan"),
                    new(17, "idtariflimbah"),
                    new(18, "idtariflltt"),
                    new(19, "flagangsur"),
                    new(20, "idangsuran"),
                    new(21, "termin"),
                    new(22, "flagmanual"),
                    new(23, "idpermohonansambunganbaru"),
                    new(24, "flaghapus"),
                    new(25, "iduser"),
                    new(26, "waktuupdate"),
                    new(27, "created_at"),
                    new(28, "urutan"),
                }
            },
            {
                "rekening_nonair_detail",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idnonair"),
                    new(2, "parameter"),
                    new(3, "postbiaya"),
                    new(4, "value"),
                    new(5, "waktuupdate"),
                }
            },
            {
                "rekening_nonair_transaksi",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idnonair"),
                    new(2, "nomortransaksi"),
                    new(3, "statustransaksi"),
                    new(4, "waktutransaksi"),
                    new(5, "tahuntransaksi"),
                    new(6, "iduser"),
                    new(7, "idloket"),
                    new(8, "idkolektiftransaksi"),
                    new(9, "idalasanbatal"),
                    new(10, "keterangan"),
                    new(11, "waktuupdate"),
                }
            },
            {
                "permohonan_non_pelanggan",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpermohonan"),
                    new(2, "idtipepermohonan"),
                    new(3, "idsumberpengaduan"),
                    new(4, "nomorpermohonan"),
                    new(5, "waktupermohonan"),
                    new(6, "flagpendaftaran"),
                    new(7, "idtipependaftaransambungan"),
                    new(8, "nama"),
                    new(9, "alamat"),
                    new(10, "idgolongan"),
                    new(11, "iddiameter"),
                    new(12, "idrayon"),
                    new(13, "idkelurahan"),
                    new(14, "idblok"),
                    new(15, "idperuntukan"),
                    new(16, "idjenisbangunan"),
                    new(17, "idkepemilikan"),
                    new(18, "idpekerjaan"),
                    new(19, "idkolektif"),
                    new(20, "idsumberair"),
                    new(21, "iddma"),
                    new(22, "iddmz"),
                    new(23, "idmerekmeter"),
                    new(24, "idkondisimeter"),
                    new(25, "idadministrasilain"),
                    new(26, "idpemeliharaanlain"),
                    new(27, "idretribusilain"),
                    new(28, "noserimeter"),
                    new(29, "tglmeter"),
                    new(30, "urutanbaca"),
                    new(31, "stanawalpasang"),
                    new(32, "notelp"),
                    new(33, "email"),
                    new(34, "noktp"),
                    new(35, "nokk"),
                    new(36, "kodepost"),
                    new(37, "dayalistrik"),
                    new(38, "luastanah"),
                    new(39, "luasrumah"),
                    new(40, "rt"),
                    new(41, "rw"),
                    new(42, "nohp"),
                    new(43, "keterangan"),
                    new(44, "nosambyangdiberikan"),
                    new(45, "nosambdepan"),
                    new(46, "nosambbelakang"),
                    new(47, "nosambkiri"),
                    new(48, "nosambkanan"),
                    new(49, "penghuni"),
                    new(50, "namapemilik"),
                    new(51, "alamatpemilik"),
                    new(52, "iduser"),
                    new(53, "idnonair"),
                    new(54, "latitude"),
                    new(55, "longitude"),
                    new(56, "alamatmap"),
                    new(57, "flagverifikasi"),
                    new(58, "waktuverifikasi"),
                    new(59, "flagpelanggankavlingan"),
                    new(60, "flaghapus"),
                    new(61, "waktuupdate"),
                    new(62, "airyangdigunakansebelumnya"),
                    new(63, "statuspermohonan"),
                }
            },
            {
                "permohonan_non_pelanggan_detail",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpermohonan"),
                    new(2, "parameter"),
                    new(3, "tipedata"),
                    new(4, "valuestring"),
                    new(5, "valuedecimal"),
                    new(6, "valueinteger"),
                    new(7, "valuedate"),
                    new(8, "valuebool"),
                    new(9, "waktuupdate"),
                }
            },
            {
                "permohonan_non_pelanggan_spk",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpermohonan"),
                    new(2, "nomorspk"),
                    new(3, "tanggalspk"),
                    new(4, "iduser"),
                    new(5, "flagsurvey"),
                    new(6, "flagbatal"),
                    new(7, "idalasanbatal"),
                    new(8, "waktuupdate"),
                }
            },
            {
                "permohonan_non_pelanggan_spk_detail",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpermohonan"),
                    new(2, "parameter"),
                    new(3, "tipedata"),
                    new(4, "valuestring"),
                    new(5, "valuedecimal"),
                    new(6, "valueinteger"),
                    new(7, "valuedate"),
                    new(8, "valuebool"),
                    new(9, "waktuupdate"),
                }
            },
            {
                "permohonan_non_pelanggan_rab",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpermohonan"),
                    new(2, "idjenisnonair"),
                    new(3, "idnonair"),
                    new(4, "nomorrab"),
                    new(5, "tanggalrab"),
                    new(6, "nomorbppi"),
                    new(7, "tanggalbppi"),
                    new(8, "iduserbppi"),
                    new(9, "iduser"),
                    new(10, "tanggalkadaluarsa"),
                    new(11, "persilnamapaket"),
                    new(12, "persilflagdialihkankevendor"),
                    new(13, "persilflagbiayadibebankankepdam"),
                    new(14, "persilsubtotal"),
                    new(15, "persildibebankankepdam"),
                    new(16, "persiltotal"),
                    new(17, "distribusinamapaket"),
                    new(18, "distribusiflagdialihkankevendor"),
                    new(19, "distribusiflagbiayadibebankankepdam"),
                    new(20, "distribusisubtotal"),
                    new(21, "distribusidibebankankepdam"),
                    new(22, "distribusitotal"),
                    new(23, "rekapsubtotal"),
                    new(24, "rekapdibebankankepdam"),
                    new(25, "rekaptotal"),
                    new(26, "flagrablainnya"),
                    new(27, "flagbatal"),
                    new(28, "idalasanbatal"),
                    new(29, "waktuupdate"),
                }
            },
            {
                "permohonan_non_pelanggan_rab_detail",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "id"),
                    new(1, "idpdam"),
                    new(2, "idpermohonan"),
                    new(3, "tipe"),
                    new(4, "kode"),
                    new(5, "uraian"),
                    new(6, "hargasatuan"),
                    new(7, "satuan"),
                    new(8, "qty"),
                    new(9, "jumlah"),
                    new(10, "ppn"),
                    new(11, "keuntungan"),
                    new(12, "jasadaribahan"),
                    new(13, "total"),
                    new(14, "kategori"),
                    new(15, "kelompok"),
                    new(16, "postbiaya"),
                    new(17, "qtyrkp"),
                    new(18, "flagbiayadibebankankepdam"),
                    new(19, "flagdialihkankevendor"),
                    new(20, "flagpaket"),
                    new(21, "flagdistribusi"),
                    new(22, "untuksppbdarispk"),
                    new(23, "waktuupdate"),
                }
            },
            {
                "permohonan_non_pelanggan_spk_pasang",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpermohonan"),
                    new(2, "nomorspk"),
                    new(3, "tanggalspk"),
                    new(4, "nomorsppb"),
                    new(5, "tanggalsppb"),
                    new(6, "iduser"),
                    new(7, "flagbatal"),
                    new(8, "idalasanbatal"),
                    new(9, "waktuupdate"),
                }
            },
            {
                "permohonan_non_pelanggan_ba",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpermohonan"),
                    new(2, "nomorba"),
                    new(3, "tanggalba"),
                    new(4, "iduser"),
                    new(5, "persilnamapaket"),
                    new(6, "persilflagdialihkankevendor"),
                    new(7, "persilflagbiayadibebankankepdam"),
                    new(8, "distribusinamapaket"),
                    new(9, "distribusiflagdialihkankevendor"),
                    new(10, "distribusiflagbiayadibebankankepdam"),
                    new(11, "flagbatal"),
                    new(12, "idalasanbatal"),
                    new(13, "flag_dari_verifikasi"),
                    new(14, "statusberitaacara"),
                    new(15, "waktuupdate"),
                }
            },
            {
                "permohonan_non_pelanggan_ba_detail",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpermohonan"),
                    new(2, "parameter"),
                    new(3, "tipedata"),
                    new(4, "valuestring"),
                    new(5, "valuedecimal"),
                    new(6, "valueinteger"),
                    new(7, "valuedate"),
                    new(8, "valuebool"),
                    new(9, "waktuupdate"),
                }
            },
            {
                "permohonan_pelanggan_air",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpermohonan"),
                    new(2, "idtipepermohonan"),
                    new(3, "idsumberpengaduan"),
                    new(4, "nomorpermohonan"),
                    new(5, "waktupermohonan"),
                    new(6, "idrayon"),
                    new(7, "idkelurahan"),
                    new(8, "idgolongan"),
                    new(9, "iddiameter"),
                    new(10, "idpelangganair"),
                    new(11, "keterangan"),
                    new(12, "iduser"),
                    new(13, "idnonair"),
                    new(14, "latitude"),
                    new(15, "longitude"),
                    new(16, "alamatmap"),
                    new(17, "flagverifikasi"),
                    new(18, "waktuverifikasi"),
                    new(19, "flagusulan"),
                    new(20, "statuspermohonan"),
                    new(21, "flaghapus"),
                    new(22, "waktuupdate"),
                }
            },
            {
                "permohonan_pelanggan_air_detail",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpermohonan"),
                    new(2, "parameter"),
                    new(3, "tipedata"),
                    new(4, "valuestring"),
                    new(5, "valuedecimal"),
                    new(6, "valueinteger"),
                    new(7, "valuedate"),
                    new(8, "valuebool"),
                    new(9, "waktuupdate"),
                }
            },
            {
                "permohonan_pelanggan_air_spk",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpermohonan"),
                    new(2, "nomorspk"),
                    new(3, "tanggalspk"),
                    new(4, "iduser"),
                    new(5, "flagsurvey"),
                    new(6, "flagbatal"),
                    new(7, "idalasanbatal"),
                    new(8, "waktuupdate"),
                }
            },
            {
                "permohonan_pelanggan_air_spk_detail",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpermohonan"),
                    new(2, "parameter"),
                    new(3, "tipedata"),
                    new(4, "valuestring"),
                    new(5, "valuedecimal"),
                    new(6, "valueinteger"),
                    new(7, "valuedate"),
                    new(8, "valuebool"),
                    new(9, "waktuupdate"),
                }
            },
            {
                "permohonan_pelanggan_air_rab",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpermohonan"),
                    new(2, "idjenisnonair"),
                    new(3, "idnonair"),
                    new(4, "nomorrab"),
                    new(5, "tanggalrab"),
                    new(6, "nomorbppi"),
                    new(7, "tanggalbppi"),
                    new(8, "iduserbppi"),
                    new(9, "iduser"),
                    new(10, "tanggalkadaluarsa"),
                    new(11, "persilnamapaket"),
                    new(12, "persilflagdialihkankevendor"),
                    new(13, "persilflagbiayadibebankankepdam"),
                    new(14, "persilsubtotal"),
                    new(15, "persildibebankankepdam"),
                    new(16, "persiltotal"),
                    new(17, "distribusinamapaket"),
                    new(18, "distribusiflagdialihkankevendor"),
                    new(19, "distribusiflagbiayadibebankankepdam"),
                    new(20, "distribusisubtotal"),
                    new(21, "distribusidibebankankepdam"),
                    new(22, "distribusitotal"),
                    new(23, "rekapsubtotal"),
                    new(24, "rekapdibebankankepdam"),
                    new(25, "rekaptotal"),
                    new(26, "flagrablainnya"),
                    new(27, "flagbatal"),
                    new(28, "idalasanbatal"),
                    new(29, "waktuupdate"),
                }
            },
            {
                "permohonan_pelanggan_air_rab_detail",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "id"),
                    new(1, "idpdam"),
                    new(2, "idpermohonan"),
                    new(3, "tipe"),
                    new(4, "kode"),
                    new(5, "uraian"),
                    new(6, "hargasatuan"),
                    new(7, "satuan"),
                    new(8, "qty"),
                    new(9, "jumlah"),
                    new(10, "ppn"),
                    new(11, "keuntungan"),
                    new(12, "jasadaribahan"),
                    new(13, "total"),
                    new(14, "kategori"),
                    new(15, "kelompok"),
                    new(16, "postbiaya"),
                    new(17, "qtyrkp"),
                    new(18, "flagbiayadibebankankepdam"),
                    new(19, "flagdialihkankevendor"),
                    new(20, "flagpaket"),
                    new(21, "flagdistribusi"),
                    new(22, "untuksppbdarispk"),
                    new(23, "waktuupdate"),
                }
            },
            {
                "permohonan_pelanggan_air_spk_pasang",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpermohonan"),
                    new(2, "nomorspk"),
                    new(3, "tanggalspk"),
                    new(4, "nomorsppb"),
                    new(5, "tanggalsppb"),
                    new(6, "iduser"),
                    new(7, "flagbatal"),
                    new(8, "idalasanbatal"),
                    new(9, "waktuupdate"),
                }
            },
            {
                "permohonan_pelanggan_air_ba",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpermohonan"),
                    new(2, "nomorba"),
                    new(3, "tanggalba"),
                    new(4, "iduser"),
                    new(5, "persilnamapaket"),
                    new(6, "persilflagdialihkankevendor"),
                    new(7, "persilflagbiayadibebankankepdam"),
                    new(8, "distribusinamapaket"),
                    new(9, "distribusiflagdialihkankevendor"),
                    new(10, "distribusiflagbiayadibebankankepdam"),
                    new(11, "flagbatal"),
                    new(12, "idalasanbatal"),
                    new(13, "flag_dari_verifikasi"),
                    new(14, "statusberitaacara"),
                    new(15, "waktuupdate"),
                }
            },
            {
                "permohonan_pelanggan_air_ba_detail",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idpermohonan"),
                    new(2, "parameter"),
                    new(3, "tipedata"),
                    new(4, "valuestring"),
                    new(5, "valuedecimal"),
                    new(6, "valueinteger"),
                    new(7, "valuedate"),
                    new(8, "valuebool"),
                    new(9, "waktuupdate"),
                }
            },
            {
                "master_pelanggan_air_riwayat_koreksi",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "idpdam"),
                    new(1, "idkoreksi"),
                    new(2, "idpermohonan"),
                    new(3, "sumberperubahan"),
                    new(4, "waktukoreksi"),
                    new(5, "jamkoreksi"),
                    new(6, "iduser"),
                    new(7, "idpelangganair"),
                    new(8, "flagverifikasi"),
                    new(9, "waktuverifikasi"),
                    new(10, "waktuupdate"),
                    new(11, "nomor"),
                }
            },
            {
                "master_pelanggan_air_riwayat_koreksi_detail",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "id"),
                    new(1, "idpdam"),
                    new(2, "idkoreksi"),
                    new(3, "parameter"),
                    new(4, "lama"),
                    new(5, "baru"),
                    new(6, "valueid"),
                }
            },
            {
                "permohonan_pelanggan_air_koreksi_rekening",
                new List<MySqlBulkCopyColumnMapping>()
                {
                    new(0, "id"),
                    new(1, "idpdam"),
                    new(2, "idpelangganair"),
                    new(3, "idperiode"),
                    new(4, "idpermohonan"),
                    new(5, "waktuusulan"),
                    new(6, "statusverifikasilapangan"),
                    new(7, "waktustatusverifikasilapangan"),
                    new(8, "keteranganstatusverifikasilapangan"),
                    new(9, "statusverifikasipusat"),
                    new(10, "waktustatusverifikasipusat"),
                    new(11, "keteranganstatusverifikasipusat"),
                    new(12, "nomorba"),
                    new(13, "stanlalu"),
                    new(14, "stanskrg"),
                    new(15, "stanangkat"),
                    new(16, "pakai"),
                    new(17, "biayapemakaian"),
                    new(18, "administrasi"),
                    new(19, "pemeliharaan"),
                    new(20, "retribusi"),
                    new(21, "pelayanan"),
                    new(22, "airlimbah"),
                    new(23, "dendapakai0"),
                    new(24, "administrasilain"),
                    new(25, "pemeliharaanlain"),
                    new(26, "retribusilain"),
                    new(27, "ppn"),
                    new(28, "meterai"),
                    new(29, "rekair"),
                    new(30, "denda"),
                    new(31, "total"),
                    new(32, "flaghanyaabonemen"),
                    new(33, "stanlalu_usulan"),
                    new(34, "stanskrg_usulan"),
                    new(35, "stanangkat_usulan"),
                    new(36, "pakai_usulan"),
                    new(37, "biayapemakaian_usulan"),
                    new(38, "administrasi_usulan"),
                    new(39, "pemeliharaan_usulan"),
                    new(40, "retribusi_usulan"),
                    new(41, "pelayanan_usulan"),
                    new(42, "airlimbah_usulan"),
                    new(43, "dendapakai0_usulan"),
                    new(44, "administrasilain_usulan"),
                    new(45, "pemeliharaanlain_usulan"),
                    new(46, "retribusilain_usulan"),
                    new(47, "meterai_usulan"),
                    new(48, "ppn_usulan"),
                    new(49, "rekair_usulan"),
                    new(50, "denda_usulan"),
                    new(51, "total_usulan"),
                    new(52, "stanlalu_baru"),
                    new(53, "stanskrg_baru"),
                    new(54, "stanangkat_baru"),
                    new(55, "pakai_baru"),
                    new(56, "biayapemakaian_baru"),
                    new(57, "administrasi_baru"),
                    new(58, "pemeliharaan_baru"),
                    new(59, "retribusi_baru"),
                    new(60, "pelayanan_baru"),
                    new(61, "airlimbah_baru"),
                    new(62, "dendapakai0_baru"),
                    new(63, "administrasilain_baru"),
                    new(64, "pemeliharaanlain_baru"),
                    new(65, "retribusilain_baru"),
                    new(66, "meterai_baru"),
                    new(67, "ppn_baru"),
                    new(68, "rekair_baru"),
                    new(69, "denda_baru"),
                    new(70, "total_baru"),
                    new(71, "statuspermohonan"),
                    new(72, "flaghapus"),
                    new(73, "waktuupdate"),
                }
            }
        };

        public class Settings : CommandSettings
        {
            [CommandOption("-i|--idpdam")]
            public int? IdPdam { get; set; }

            [CommandOption("-n|--nama-paket")]
            public Paket? NamaPaket { get; set; }

            [CommandOption("-c|--cutoff")]
            public string Cutoff { get; set; } = DateTime.Now.AddDays(-1).Date.ToString(format: "yyyy-MM-dd");
        }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            settings.IdPdam ??= AnsiConsole.Ask<int>("ID PDAM:");
            settings.NamaPaket ??= AnsiConsole.Prompt(
                new SelectionPrompt<Paket>()
                .Title("Pilih Paket:")
                .AddChoices([Paket.Bacameter, Paket.Basic]));

            switch (settings.NamaPaket)
            {
                case Paket.Bacameter:
                    {
                        await Bacameter(settings);
                        break;
                    }
                case Paket.Basic:
                    {
                        await Basic(settings);
                        break;
                    }
                default:
                    break;
            }

            return 0;
        }

        private async Task<int> Basic(Settings settings)
        {
            const string PROSES_DATA_MASTER_BSHPD = "Proses data master bshpd";
            const string PROSES_DATA_PELANGGAN = "Proses data pelanggan";
            const string PROSES_PIUTANG_BAYAR_3_BULAN = "Proses piutang & bayar 3 bulan";
            const string PROSES_NONAIR_3_BULAN = "Proses nonair 3 bulan";
            const string PROSES_PERMOHONAN_SAMBUNG_BARU = "Proses permohonan sambung baru";
            const string PROSES_PERMOHONAN_BALIK_NAMA = "Proses permohonan balik nama";
            const string PROSES_PERMOHONAN_BUKA_SEGEL = "Proses permohonan buka segel";
            const string PROSES_PERMOHONAN_KOREKSI_DATA = "Proses permohonan koreksi data";
            const string PROSES_PERMOHONAN_KOREKSI_REKENING = "Proses permohonan koreksi rekening";
            const string PROSES_PERMOHONAN_TUTUP_TOTAL = "Proses permohonan tutup total";
            const string PROSES_PERMOHONAN_RUBAH_TARIF = "Proses permohonan rubah tarif";
            const string PROSES_PERMOHONAN_RUBAH_RAYON = "Proses permohonan rubah rayon";
            const string PROSES_PERMOHONAN_SAMBUNG_KEMBALI = "Proses permohonan sambung kembali";

            List<string> prosesList =
            [
                PROSES_DATA_MASTER_BSHPD,
                PROSES_DATA_PELANGGAN,
                PROSES_PIUTANG_BAYAR_3_BULAN,
                PROSES_NONAIR_3_BULAN,
                PROSES_PERMOHONAN_SAMBUNG_BARU,
                PROSES_PERMOHONAN_BALIK_NAMA,
                PROSES_PERMOHONAN_BUKA_SEGEL,
                PROSES_PERMOHONAN_KOREKSI_DATA,
                PROSES_PERMOHONAN_KOREKSI_REKENING,
                PROSES_PERMOHONAN_TUTUP_TOTAL,
                PROSES_PERMOHONAN_RUBAH_TARIF,
                PROSES_PERMOHONAN_RUBAH_RAYON,
                PROSES_PERMOHONAN_SAMBUNG_KEMBALI,
            ];

            string? namaPdam = "";
            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                namaPdam = await conn.QueryFirstOrDefaultAsync<string>(@"SELECT namapdam FROM master_attribute_pdam WHERE idpdam=@idpdam", new { idpdam = settings.IdPdam }, trans);
            });

            AnsiConsole.WriteLine();
            AnsiConsole.WriteLine($"Environment: {AppSettings.Environment}");
            AnsiConsole.WriteLine($"{settings.IdPdam} {namaPdam}");
            AnsiConsole.WriteLine();

            var selectedProses = AnsiConsole.Prompt(
                new MultiSelectionPrompt<string>()
                .Title("Pilih proses")
                .NotRequired()
                .InstructionsText("[grey](Press [blue]<space>[/] to toggle, [green]<enter>[/] to accept)[/]")
                .AddChoices(prosesList));

            var prosesMasterBshpd = selectedProses.Exists(s => s == PROSES_DATA_MASTER_BSHPD);
            var prosesPelanggan = selectedProses.Exists(s => s == PROSES_DATA_PELANGGAN);
            var prosesPiutangBayar3Bulan = selectedProses.Exists(s => s == PROSES_PIUTANG_BAYAR_3_BULAN);
            var prosesNonair3Bulan = selectedProses.Exists(s => s == PROSES_NONAIR_3_BULAN);
            var prosesPermohonanSambungBaru = selectedProses.Exists(s => s == PROSES_PERMOHONAN_SAMBUNG_BARU);
            var prosesPermohonanBalikNama = selectedProses.Exists(s => s == PROSES_PERMOHONAN_BALIK_NAMA);
            var prosesPermohonanBukaSegel = selectedProses.Exists(s => s == PROSES_PERMOHONAN_BUKA_SEGEL);
            var prosesPermohonanKoreksiData = selectedProses.Exists(s => s == PROSES_PERMOHONAN_KOREKSI_DATA);
            var prosesPermohonanKoreksiRekening = selectedProses.Exists(s => s == PROSES_PERMOHONAN_KOREKSI_REKENING);
            var prosesPermohonanTutupTotal = selectedProses.Exists(s => s == PROSES_PERMOHONAN_TUTUP_TOTAL);
            var prosesPermohonanRubahTarif = selectedProses.Exists(s => s == PROSES_PERMOHONAN_RUBAH_TARIF);
            var prosesPermohonanRubahRayon = selectedProses.Exists(s => s == PROSES_PERMOHONAN_RUBAH_RAYON);
            var prosesPermohonanSambungKembali = selectedProses.Exists(s => s == PROSES_PERMOHONAN_SAMBUNG_KEMBALI);

            var periodeMulai = AnsiConsole.Prompt(
                new TextPrompt<string>("Periode mulai (yyyyMM):"));

            AnsiConsole.WriteLine();
            AnsiConsole.WriteLine($"Paket: {settings.NamaPaket}");
            AnsiConsole.WriteLine($"Periode mulai: {periodeMulai}");
            AnsiConsole.WriteLine("Proses dipilih:");
            AnsiConsole.Write(new Rows(selectedProses.Select(s => new Text($"- {s}")).ToList()));
            AnsiConsole.WriteLine();

            if (!Utils.ConfirmationPrompt("Yakin untuk melanjutkan?"))
            {
                return 0;
            }

            try
            {
                await Utils.MainConnectionWrapper(async (conn, trans) =>
                {
                    await conn.ExecuteAsync(
                        sql: @"
                        SET GLOBAL foreign_key_checks=0;
                        SET GLOBAL innodb_flush_log_at_trx_commit=2;
                        SET GLOBAL max_allowed_packet = 1073741824; -- 1GB",
                        transaction: trans);
                });

                await Utils.LoketConnectionWrapper(async (conn, trans) =>
                {
                    await conn.ExecuteAsync(
                        sql: @"
                        SET GLOBAL max_allowed_packet = 1073741824; -- 1GB",
                        transaction: trans);
                });

                await AnsiConsole.Status()
                    .StartAsync("🚀", async _ =>
                    {
                        if (prosesMasterBshpd)
                        {
                            Utils.WriteLogMessage("Proses data master bshpd");
                            await MasterData(settings);
                            await PaketMaterial(settings);
                            await PaketOngkos(settings);
                            await PaketRab(settings);
                        }

                        Utils.WriteLogMessage("Copy data master bshpd ke db tampung");
                        await LoadDataMaster(settings);

                        if (prosesPelanggan)
                        {
                            Utils.WriteLogMessage("Proses data pelanggan");
                            await Utils.TrackProgress("master_pelanggan_air", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.LoketConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_pelanggan_air",
                                    queryPath: @"Queries\master_pelanggan_air.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    },
                                    placeholders: new()
                                    {
                                        { "[bacameter]", AppSettings.DatabaseBacameter },
                                        { "[bsbs]", AppSettings.DatabaseBsbs },
                                        { "[dataawal]", AppSettings.DataAwalDatabase },
                                    },
                                    columnMappings: ColumnMappings["master_pelanggan_air"]);
                            });
                            await Utils.TrackProgress("master_pelanggan_air_detail", async () =>
                            {
                                await Utils.MainConnectionWrapper(async (conn, trans) =>
                                {
                                    await conn.ExecuteAsync(
                                        sql: @"
                                        ALTER TABLE master_pelanggan_air_detail
                                        CHANGE alamatpemilik alamatpemilik VARCHAR (250) CHARSET latin1 COLLATE latin1_swedish_ci NULL",
                                        transaction: trans);
                                });

                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.LoketConnectionString,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_pelanggan_air_detail",
                                    queryPath: @"Queries\master_pelanggan_air_detail.sql",
                                    parameters: new()
                                    {
                                        { "@idpdam", settings.IdPdam }
                                    },
                                    placeholders: new()
                                    {
                                        { "[dataawal]", AppSettings.DataAwalDatabase },
                                    },
                                    columnMappings: ColumnMappings["master_pelanggan_air_detail"]);
                            });
                        }

                        if (prosesPiutangBayar3Bulan)
                        {
                            await Utils.BacameterConnectionWrapper(async (conn, trans) =>
                            {
                                await conn.ExecuteAsync(
                                    sql: @"
                                    DROP TABLE IF EXISTS tampung_hasilbaca;
                                    CREATE TABLE tampung_hasilbaca AS
                                    SELECT * FROM hasilbaca LIMIT 0;
                                    ALTER TABLE `tampung_hasilbaca`
                                    ADD COLUMN `kode` VARCHAR (100) NOT NULL FIRST,
                                    ADD PRIMARY KEY (`kode`);",
                                    transaction: trans);

                                for (int i = 0; i < 3; i++)
                                {
                                    var periode = DateTime.ParseExact(periodeMulai, "yyyyMM", null).AddMonths(i);
                                    Utils.WriteLogMessage($"Ambil data hasilbaca{periode:MMyy} ke tampung_hasilbaca");
                                    await conn.ExecuteAsync(
                                        sql: $@"
                                        INSERT INTO tampung_hasilbaca
                                        SELECT
                                            CONCAT({periode:yyyyMM},'.',`idpelanggan`) AS kode,
                                            `idpelanggan`,
                                            `idmeteran`,
                                            `idkec`,
                                            `kec`,
                                            `idkel`,
                                            `kel`,
                                            `idrtrw`,
                                            `rtrw`,
                                            `idblok`,
                                            `kodeblok`,
                                            `blok`,
                                            `idrayon`,
                                            `koderayon`,
                                            `rayon`,
                                            `wilayah`,
                                            `nama`,
                                            `noktp`,
                                            `telprumah`,
                                            `alamat`,
                                            `pekerjaan`,
                                            `nosambungan`,
                                            `nometer`,
                                            `tekanan`,
                                            `idgol`,
                                            `kodegol`,
                                            `golongan`,
                                            `idgol1`,
                                            `kodegol1`,
                                            `golongan1`,
                                            `perubahangol`,
                                            `golonganlalu`,
                                            `iddiameter`,
                                            `kodediameter`,
                                            `ukuran`,
                                            `tgldaftar`,
                                            `luasrumah`,
                                            `urutanbaca`,
                                            `prosesairlimbah`,
                                            `keterangan`,
                                            `bln1`,
                                            `stan1`,
                                            `pakai1`,
                                            `persen1`,
                                            `bln2`,
                                            `stan2`,
                                            `pakai2`,
                                            `persen2`,
                                            `bln3`,
                                            `stan3`,
                                            `pakai3`,
                                            `persen3`,
                                            `stanlalu`,
                                            `stanskrg`,
                                            `pakaiskrg`,
                                            `stanangkat`,
                                            `persentase`,
                                            `taksir`,
                                            `taksir2bln`,
                                            `taksir3bln`,
                                            `idkelainan`,
                                            `kodekelainan`,
                                            `kelainan`,
                                            `kelainanlalu`,
                                            `idkelainan1`,
                                            `kodekelainan1`,
                                            `kelainan1`,
                                            `kelainan1lalu`,
                                            `kelainan1lalu1`,
                                            `tunggakan`,
                                            `dendatunggakan`,
                                            `biayapemakaian`,
                                            `airlimbah`,
                                            `administrasi`,
                                            `bebanpasif`,
                                            `retribusi`,
                                            `pemeliharaan`,
                                            `pelayanan`,
                                            `meterai`,
                                            `custombeban1`,
                                            `custombeban2`,
                                            `custombeban3`,
                                            `persenppn`,
                                            `ppn`,
                                            `totalrekening`,
                                            `sudahlunas`,
                                            `rincianrekening`,
                                            `sudahbaca`,
                                            `verifikasi`,
                                            `waktuverifikasi`,
                                            `a`,
                                            `idpetugas`,
                                            `kodepetugas`,
                                            `namapetugas`,
                                            `waktubaca`,
                                            `waktubacalalu`,
                                            `waktuupload`,
                                            `sumberlokasi`,
                                            `latitude`,
                                            `longitude`,
                                            `mnc`,
                                            `mcc`,
                                            `lac`,
                                            `cellid`,
                                            `adafotorumah`,
                                            `adavideo`,
                                            `lampiran`,
                                            `flagkirimsms`,
                                            `sudahkirimsms`,
                                            `logupdate`,
                                            `iduserupdate`,
                                            `flagsudahupload`,
                                            `flagaktif`,
                                            `custom1`,
                                            `custom2`,
                                            `peruntukan`,
                                            `hasilbacaulang`,
                                            `totalrekeningstr`,
                                            `datalapangan`,
                                            `ratarata3bln`,
                                            `flaghistori3bln`,
                                            `terbaca`,
                                            `memolapangan`,
                                            `wm`,
                                            `masterlatlong`,
                                            `flagkoreksi`
                                        FROM
                                            hasilbaca{periode:MMyy}",
                                        transaction: trans);
                                }
                            });

                            Utils.WriteLogMessage("Proses data piutang");
                            await Piutang(settings);

                            Utils.WriteLogMessage("Proses data bayar");
                            await Bayar(settings);
                        }

                        if (prosesNonair3Bulan)
                        {
                            Utils.WriteLogMessage("Proses data nonair 3 bulan");
                            await Nonair(settings);
                        }

                        if (prosesPermohonanSambungBaru)
                        {
                            Utils.WriteLogMessage("Proses data permohonan sambung baru");
                            await SambungBaru(settings);
                        }

                        if (prosesPermohonanBalikNama)
                        {
                            Utils.WriteLogMessage("Proses data permohonan balik nama");
                            await BalikNama(settings);
                        }

                        if (prosesPermohonanBukaSegel)
                        {
                            Utils.WriteLogMessage("Proses data permohonan balik nama");
                            await BukaSegel(settings);
                        }

                        if (prosesPermohonanKoreksiData)
                        {
                            Utils.WriteLogMessage("Proses data permohonan koreksi data");
                            await KoreksiData(settings);
                        }

                        if (prosesPermohonanKoreksiRekening)
                        {
                            Utils.WriteLogMessage("Proses data permohonan koreksi rekening");
                            await KoreksiRekair(settings);
                        }

                        if (prosesPermohonanTutupTotal)
                        {
                            Utils.WriteLogMessage("Proses data permohonan tutup total");
                            await TutupTotal(settings);
                        }

                        if (prosesPermohonanRubahTarif)
                        {
                            Utils.WriteLogMessage("Proses data permohonan rubah tarif");
                            await RubahTarif(settings);
                        }

                        if (prosesPermohonanRubahRayon)
                        {
                            Utils.WriteLogMessage("Proses data permohonan rubah rayon");
                            await RubahRayon(settings);
                        }

                        if (prosesPermohonanSambungKembali)
                        {
                            Utils.WriteLogMessage("Proses data permohonan sambung kembali");
                            await SambungKembali(settings);
                        }

                        if (false)
                        {
                            await Report(settings);

                            await Utils.TrackProgress("bayar tahun", async () =>
                            {
                                await BayarTahun(settings);
                            });
                            await Utils.TrackProgress("nonair tahun", async () =>
                            {
                                await NonairTahun(settings);
                            });
                            await Utils.TrackProgress("angsuran air", async () =>
                            {
                                await AngsuranAir(settings);
                            });
                            await Utils.TrackProgress("angsuran nonair", async () =>
                            {
                                await AngsuranNonair(settings);
                            });
                            await Utils.TrackProgress("pengaduan pelanggan", async () =>
                            {
                                await PengaduanPelanggan(settings);
                            });
                            await Utils.TrackProgress("pengaduan non pelanggan", async () =>
                            {
                                await PengaduanNonPelanggan(settings);
                            });
                            await Utils.TrackProgress("air tangki non pelanggan", async () =>
                            {
                                await AirTangkiNonPelanggan(settings);
                            });
                            await Utils.TrackProgress("air tangki pelanggan", async () =>
                            {
                                await AirTangkiPelanggan(settings);
                            });
                            await Utils.TrackProgress("rotasimeter", async () =>
                            {
                                await Rotasimeter(settings);
                            });
                            await Utils.TrackProgress("rotasimeter nonrutin", async () =>
                            {
                                await RotasimeterNonrutin(settings);
                            });
                            await Utils.TrackProgress("rab lainnya pelanggan", async () =>
                            {
                                await RabLainnyaPelanggan(settings);
                            });

                        }
                    });

                AnsiConsole.MarkupLine("");
                AnsiConsole.MarkupLine($"[bold green]Migrasi data basic finish.[/]");

                return 0;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                await Utils.MainConnectionWrapper(async (conn, trans) =>
                {
                    await conn.ExecuteAsync(
                        sql: @"
                        SET GLOBAL foreign_key_checks = 1;
                        SET GLOBAL innodb_flush_log_at_trx_commit = 1;",
                        transaction: trans);
                });
            }
        }
        private static async Task<int> Bacameter(Settings settings)
        {
            const string PROSES_DATA_MASTER = "Proses data master";
            const string PROSES_DATA_PELANGGAN = "Proses data pelanggan";
            const string PROSES_REKENING = "Proses rekening";

            List<string> prosesList =
            [
                PROSES_DATA_MASTER,
                PROSES_DATA_PELANGGAN,
                PROSES_REKENING,
            ];

            string? namaPdam = "";
            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                namaPdam = await conn.QueryFirstOrDefaultAsync<string>(@"SELECT namapdam FROM master_attribute_pdam WHERE idpdam=@idpdam", new { idpdam = settings.IdPdam }, trans);
            });

            AnsiConsole.WriteLine();
            AnsiConsole.WriteLine($"Environment: {AppSettings.Environment}");
            AnsiConsole.WriteLine($"{settings.IdPdam} {namaPdam}");
            AnsiConsole.WriteLine();

            var selectedProses = AnsiConsole.Prompt(
                new MultiSelectionPrompt<string>()
                .Title("Pilih proses")
                .NotRequired()
                .InstructionsText("[grey](Press [blue]<space>[/] to toggle, [green]<enter>[/] to accept)[/]")
                .AddChoices(prosesList));

            var prosesMaster = selectedProses.Exists(s => s == PROSES_DATA_MASTER);
            var prosesPelanggan = selectedProses.Exists(s => s == PROSES_DATA_PELANGGAN);
            var prosesRekening = selectedProses.Exists(s => s == PROSES_REKENING);

            AnsiConsole.WriteLine();
            AnsiConsole.WriteLine($"Paket: {settings.NamaPaket}");
            AnsiConsole.WriteLine("Proses dipilih:");
            AnsiConsole.Write(new Rows(selectedProses.Select(s => new Text($"- {s}")).ToList()));
            AnsiConsole.WriteLine();

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
                            await Utils.BsbsConnectionWrapper(async (conn, trans) =>
                            {
                                var cek = await conn.QueryFirstOrDefaultAsync<int?>("SELECT 1 FROM information_schema.COLUMNS WHERE table_schema=@schema AND table_name='pelanggan' AND column_name='id'",
                                    new { schema = AppSettings.DatabaseBsbs }, trans);
                                if (cek is null)
                                {
                                    var query = await File.ReadAllTextAsync(@"Queries\patches\tambah_field_id_tabel_pelanggan.sql");
                                    await conn.ExecuteAsync(query, transaction: trans, commandTimeout: AppSettings.CommandTimeout);
                                }
                            });
                        });

                        if (prosesMaster)
                        {
                            await Utils.TrackProgress("master_attribute_flag", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    queryPath: @"Queries\bacameter\master_attribute_flag.sql",
                                    table: "master_attribute_flag",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_status", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_status",
                                    queryPath: @"Queries\bacameter\master_attribute_status.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_jenis_bangunan", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_jenis_bangunan",
                                    queryPath: @"Queries\bacameter\master_attribute_jenis_bangunan.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_kepemilikan", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_kepemilikan",
                                    queryPath: @"Queries\bacameter\master_attribute_kepemilikan.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_pekerjaan", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_pekerjaan",
                                    queryPath: @"Queries\bacameter\master_attribute_pekerjaan.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_peruntukan", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_peruntukan",
                                    queryPath: @"Queries\bacameter\master_attribute_peruntukan.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_jenis_pipa", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_jenis_pipa",
                                    queryPath: @"Queries\bacameter\master_attribute_jenis_pipa.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_kwh", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_kwh",
                                    queryPath: @"Queries\bacameter\master_attribute_kwh.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_tarif_golongan", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_tarif_golongan",
                                    queryPath: @"Queries\bacameter\master_tarif_golongan.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_tarif_golongan_detail", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_tarif_golongan_detail",
                                    queryPath: @"Queries\bacameter\master_tarif_golongan_detail.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_tarif_diameter", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_tarif_diameter",
                                    queryPath: @"Queries\bacameter\master_tarif_diameter.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_tarif_diameter_detail", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_tarif_diameter_detail",
                                    queryPath: @"Queries\bacameter\master_tarif_diameter_detail.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_wilayah", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_wilayah",
                                    queryPath: @"Queries\bacameter\master_attribute_wilayah.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_area", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_area",
                                    queryPath: @"Queries\bacameter\master_attribute_area.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_rayon", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_rayon",
                                    queryPath: @"Queries\bacameter\master_attribute_rayon.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_blok", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_blok",
                                    queryPath: @"Queries\bacameter\master_attribute_blok.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_cabang", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_cabang",
                                    queryPath: @"Queries\bacameter\master_attribute_cabang.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_kecamatan", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_kecamatan",
                                    queryPath: @"Queries\bacameter\master_attribute_kecamatan.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_kelurahan", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_kelurahan",
                                    queryPath: @"Queries\bacameter\master_attribute_kelurahan.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_dma", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_dma",
                                    queryPath: @"Queries\bacameter\master_attribute_dma.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_dmz", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_dmz",
                                    queryPath: @"Queries\bacameter\master_attribute_dmz.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_tarif_administrasi_lain", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_tarif_administrasi_lain",
                                    queryPath: @"Queries\bacameter\master_tarif_administrasi_lain.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_tarif_pemeliharaan_lain", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_tarif_pemeliharaan_lain",
                                    queryPath: @"Queries\bacameter\master_tarif_pemeliharaan_lain.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_tarif_retribusi_lain", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_tarif_retribusi_lain",
                                    queryPath: @"Queries\bacameter\master_tarif_retribusi_lain.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_kolektif", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_kolektif",
                                    queryPath: @"Queries\bacameter\master_attribute_kolektif.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_sumber_air", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_sumber_air",
                                    queryPath: @"Queries\bacameter\master_attribute_sumber_air.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_merek_meter", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_merek_meter",
                                    queryPath: @"Queries\bacameter\master_attribute_merek_meter.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_kondisi_meter", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_kondisi_meter",
                                    queryPath: @"Queries\bacameter\master_attribute_kondisi_meter.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_kelainan", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBacameter,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_kelainan",
                                    queryPath: @"Queries\bacameter\master_attribute_kelainan.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_petugas_baca", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBacameter,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_attribute_petugas_baca",
                                    queryPath: @"Queries\bacameter\master_attribute_petugas_baca.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_periode", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_periode",
                                    queryPath: @"Queries\bacameter\master_periode.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_periode_billing", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_periode_billing",
                                    queryPath: @"Queries\bacameter\master_periode_billing.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                            await Utils.TrackProgress("master_attribute_jadwal_baca", async () =>
                            {
                                await Utils.BacameterConnectionWrapper(async (conn, trans) =>
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
                                        await Utils.MainConnectionWrapper(async (conn, trans) =>
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
                        }

                        if (prosesPelanggan)
                        {
                            await Utils.TrackProgress("master_pelanggan_air", async () =>
                            {
                                await Utils.BulkCopy(
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_pelanggan_air",
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
                                    sourceConnection: AppSettings.ConnectionStringBsbs,
                                    targetConnection: AppSettings.MainConnectionString,
                                    table: "master_pelanggan_air_detail",
                                    queryPath: @"Queries\bacameter\master_pelanggan_air_detail.sql",
                                    parameters: new()
                                    {
                                                { "@idpdam", settings.IdPdam }
                                    });
                            });
                        }

                        if (prosesRekening)
                        {
                            var lastId = 0;
                            await Utils.MainConnectionWrapper(async (conn, trans) =>
                            {
                                lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                            });

                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringBsbs,
                                targetConnection: AppSettings.MainConnectionString,
                                table: "rekening_air",
                                queryPath: @"Queries\bacameter\drd.sql",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                    { "@lastid", lastId },
                                },
                                placeholders: new()
                                {
                                    { "[bacameter]", AppSettings.DatabaseBacameter },
                                    { "[bsbs]", AppSettings.DatabaseBsbs },
                                    { "[loket]", AppSettings.LoketDatabase },
                                });

                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.ConnectionStringBsbs,
                                targetConnection: AppSettings.MainConnectionString,
                                table: "rekening_air_detail",
                                queryPath: @"Queries\bacameter\drd_detail.sql",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam }
                                },
                                placeholders: new()
                                {
                                    { "[bacameter]", AppSettings.DatabaseBacameter },
                                    { "[bsbs]", AppSettings.DatabaseBsbs },
                                    { "[loket]", AppSettings.LoketDatabase },
                                });
                        }
                    });

                AnsiConsole.MarkupLine("");
                AnsiConsole.MarkupLine($"[bold green]Migrasi data bacameter finish.[/]");

                return 0;
            }
            catch (Exception)
            {
                throw;
            }
        }
        private static async Task LoadDataMaster(Settings settings)
        {
            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_blok",
                query: @"select * from master_attribute_blok where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_jenis_bangunan",
                query: @"select * from master_attribute_jenis_bangunan where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_jenis_nonair",
                query: @"select * from master_attribute_jenis_nonair where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_kelainan",
                query: @"select * from master_attribute_kelainan where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_kelurahan",
                query: @"select * from master_attribute_kelurahan where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_kepemilikan",
                query: @"select * from master_attribute_kepemilikan where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_kolektif",
                query: @"select * from master_attribute_kolektif where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_kondisi_meter",
                query: @"select * from master_attribute_kondisi_meter where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_kwh",
                query: @"select * from master_attribute_kwh where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_loket",
                query: @"select * from master_attribute_loket where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_material",
                query: @"select * from master_attribute_material where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_material_paket",
                query: @"select * from master_attribute_material_paket where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_material_paket_detail",
                query: @"select * from master_attribute_material_paket_detail where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_merek_meter",
                query: @"select * from master_attribute_merek_meter where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_ongkos",
                query: @"select * from master_attribute_ongkos where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_ongkos_paket",
                query: @"select * from master_attribute_ongkos_paket where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_ongkos_paket_detail",
                query: @"select * from master_attribute_ongkos_paket_detail where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_paket",
                query: @"select * from master_attribute_paket where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_pekerjaan",
                query: @"select * from master_attribute_pekerjaan where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_peruntukan",
                query: @"
                SELECT
                `idpdam`,
                `idperuntukan`,
                `namaperuntukan`,
                `flaghapus`,
                `waktuupdate`
                FROM master_attribute_peruntukan where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                columnMappings:
                [
                    new(0,"idpdam"),
                    new(1,"idperuntukan"),
                    new(2,"namaperuntukan"),
                    new(3,"flaghapus"),
                    new(4,"waktuupdate"),
                ]);

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_petugas_baca",
                query: @"select * from master_attribute_petugas_baca where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_rayon",
                query: @"select * from master_attribute_rayon where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_sumber_air",
                query: @"select * from master_attribute_sumber_air where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_tipe_pendaftaran_sambungan",
                query: @"select * from master_attribute_tipe_pendaftaran_sambungan where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_tipe_permohonan",
                query: @"
                SELECT
                `idpdam`,
                `idtipepermohonan`,
                `kodetipepermohonan`,
                `namatipepermohonan`,
                `idjenisnonair`,
                `kategori`,
                `flagpelangganair`,
                `flagpelangganlimbah`,
                `flagpelangganlltt`,
                `flagnonpelanggan`,
                `flagpermohonanpelanggannonaktif`,
                `step_spk`,
                `step_rab`,
                `step_spkpasang`,
                `step_beritaacara`,
                `step_verifikasi`,
                `kolektif`,
                `listselainidstatus`,
                `idurusan`,
                `tipecekpiutang`,
                `jumlahpiutang`,
                `listblokpermohonan`,
                `flagaktif`,
                `flaghapus`,
                `waktuupdate`
                FROM master_attribute_tipe_permohonan where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_attribute_warna_segel",
                query: @"
                REPLACE INTO master_attribute_warna_segel VALUES
                (@idpdam,-1,'-',0,NOW()),
                (@idpdam,1,'Biru',0,NOW()),
                (@idpdam,2,'Hijau',0,NOW()),
                (@idpdam,3,'Kuning',0,NOW()),
                (@idpdam,4,'Merah',0,NOW()),
                (@idpdam,5,'Orange',0,NOW());

                SELECT * FROM master_attribute_warna_segel;",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_periode",
                query: @"select * from master_periode where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_tarif_diameter",
                query: @"select * from master_tarif_diameter where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_tarif_golongan",
                query: @"select * from master_tarif_golongan where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "master_user",
                query: @"select * from master_user where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "personalia_master_attribute_urusan_pegawai",
                query: @"select * from personalia_master_attribute_urusan_pegawai where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "tampung_master_pelanggan_air",
                query: @"SELECT idpdam,`idpelangganair`,`nosamb` FROM `master_pelanggan_air` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "tampung_rekening_nonair",
                query: @"SELECT idpdam,`idnonair`,`nomornonair`,urutan FROM `rekening_nonair` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "tampung_permohonan_non_pelanggan",
                query: @"SELECT idpdam,`idpermohonan`,idtipepermohonan,`nomorpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "tampung_permohonan_pelanggan_air",
                query: @"SELECT idpdam,`idpermohonan`,idtipepermohonan,`nomorpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "tampung_koreksi_data",
                query: @"SELECT idpdam,`idkoreksi`,nomor FROM `master_pelanggan_air_riwayat_koreksi` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

        }
        private static async Task Nonair(Settings settings)
        {
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "rekening_nonair",
                queryPath: @"Queries\nonair\nonair.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["rekening_nonair"]);

            //copy terbaru
            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "tampung_rekening_nonair",
                query: @"SELECT idpdam,`idnonair`,`nomornonair`,urutan FROM `rekening_nonair` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "rekening_nonair_detail",
                queryPath: @"Queries\nonair\nonair_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["rekening_nonair_detail"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "rekening_nonair_transaksi",
                queryPath: @"Queries\nonair\nonair_transaksi.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["rekening_nonair_transaksi"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "rekening_nonair_transaksi",
                queryPath: @"Queries\nonair\nonair_transaksi_batal.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["rekening_nonair_transaksi"]);
        }
        private async Task NonairTahun(Settings settings)
        {
            IEnumerable<string?> nonairTahun = [];
            await Utils.LoketConnectionWrapper(async (conn, trans) =>
            {
                nonairTahun = await conn.QueryAsync<string?>(
                    sql: @"SELECT RIGHT(table_name, 4) FROM information_schema.TABLES WHERE table_schema=@table_schema AND table_name RLIKE 'nonair[0-9]{4}'",
                    param: new
                    {
                        table_schema = AppSettings.LoketDatabase
                    },
                    transaction: trans);
            });

            foreach (var tahun in nonairTahun)
            {
                await Utils.TrackProgress($"nonair{tahun}", async () =>
                {
                    IEnumerable<int>? listPeriode = [];
                    await Utils.LoketConnectionWrapper(async (conn, trans) =>
                    {
                        listPeriode = await conn.QueryAsync<int>(
                            sql: $@"
                            SELECT
                            a.periode
                            FROM (
                            SELECT
                            CASE WHEN periode IS NULL OR periode='' THEN -1 ELSE periode END AS periode
                            FROM nonair{tahun}
                            WHERE DATE(COALESCE(`waktuinput`,`waktuupdate`))<=@cutoff
                            GROUP BY periode
                            ) a GROUP BY a.periode",
                            param: new
                            {
                                cutoff = settings.Cutoff
                            },
                            transaction: trans);
                    });

                    IEnumerable<dynamic>? jenis = [];

                    var lastId = 0;
                    await Utils.MainConnectionWrapper(async (conn, trans) =>
                    {
                        lastId = await conn.QueryFirstOrDefaultAsync<int>(@"SELECT IFNULL(MAX(idnonair),0) FROM rekening_nonair", transaction: trans);
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

                    await Utils.LoketConnectionWrapper(async (conn, trans) =>
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
                                sourceConnection: AppSettings.LoketConnectionString,
                                targetConnection: AppSettings.MainConnectionString,
                                table: "rekening_nonair",
                                queryPath: @"Queries\nonair\nonair.sql",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                    { "@periode", periode },
                                    { "@lastid", lastId },
                                    { "@cutoff", settings.Cutoff },
                                },
                                placeholders: new()
                                {
                                    { "[table]", $"nonair{tahun}" },
                                    { "[bsbs]", AppSettings.DatabaseBsbs },
                                });
                        });

                        await Utils.TrackProgress($"nonair{tahun}-{periode}|rekening_nonair_detail", async () =>
                        {
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.LoketConnectionString,
                                targetConnection: AppSettings.MainConnectionString,
                                table: "rekening_nonair_detail",
                                queryPath: @"Queries\nonair\nonair_detail.sql",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                    { "@periode", periode },
                                    { "@lastid", lastId },
                                    { "@cutoff", settings.Cutoff },
                                },
                                placeholders: new()
                                {
                                    { "[table]", $"nonair{tahun}" },
                                });
                        });

                        await Utils.TrackProgress($"nonair{tahun}-{periode}|rekening_nonair_transaksi", async () =>
                        {
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.LoketConnectionString,
                                targetConnection: AppSettings.MainConnectionString,
                                table: "rekening_nonair_transaksi",
                                queryPath: @"Queries\nonair\nonair_transaksi.sql",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                    { "@periode", periode },
                                    { "@lastid", lastId },
                                    { "@cutoff", settings.Cutoff },
                                },
                                placeholders: new()
                                {
                                    { "[table]", $"nonair{tahun}" },
                                    { "[bacameter]", AppSettings.DatabaseBacameter },
                                    { "[bsbs]", AppSettings.DatabaseBsbs },
                                });
                        });
                    }

                    await Utils.TrackProgress($"nonair{tahun}-batal|rekening_nonair_transaksi", async () =>
                    {
                        await Utils.BulkCopy(
                            sourceConnection: AppSettings.LoketConnectionString,
                            targetConnection: AppSettings.MainConnectionString,
                            table: "rekening_nonair_transaksi",
                            queryPath: @"Queries\nonair\nonair_transaksi_batal.sql",
                            parameters: new()
                            {
                                { "@idpdam", settings.IdPdam },
                                { "@lastid", lastId },
                                { "@cutoff", settings.Cutoff },
                            },
                            placeholders: new()
                            {
                                { "[table]", $"nonair{tahun}" },
                                { "[bacameter]", AppSettings.DatabaseBacameter },
                                { "[bsbs]", AppSettings.DatabaseBsbs },
                            });
                    });
                });
            }

            await Utils.TrackProgress($"nonair tahun-patch", async () =>
            {
                await Utils.MainConnectionWrapper(async (conn, trans) =>
                {
                    await conn.ExecuteAsync(
                        sql: await File.ReadAllTextAsync(@"Queries\nonair\patch.sql"),
                        transaction: trans,
                        commandTimeout: (int)TimeSpan.FromMinutes(15).TotalSeconds);
                });
            });

            await Utils.LoketConnectionWrapper(async (conn, trans) =>
            {
                await conn.ExecuteAsync(sql: @"DROP TABLE IF EXISTS __tmp_jenisnonair", transaction: trans);
            });
        }
        private static async Task Piutang(Settings settings)
        {
            var lastId = 0;
            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
            });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "rekening_air",
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
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["rekening_air"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "rekening_air_detail",
                queryPath: @"Queries\piutang\piutang_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["rekening_air_detail"]);
        }
        private async Task BayarTahun(Settings settings)
        {
            IEnumerable<string?> bayarTahun = [];
            await Utils.LoketConnectionWrapper(async (conn, trans) =>
            {
                bayarTahun = await conn.QueryAsync<string?>(
                    sql: @"SELECT RIGHT(table_name, 4) FROM information_schema.TABLES WHERE table_schema=@table_schema AND table_name RLIKE 'bayar[0-9]{4}'",
                    param: new { table_schema = AppSettings.LoketDatabase },
                    transaction: trans);
            });

            foreach (var tahun in bayarTahun)
            {
                await Utils.TrackProgress($"bayar{tahun}", async () =>
                {
                    IEnumerable<int>? listPeriode = [];
                    await Utils.LoketConnectionWrapper(async (conn, trans) =>
                    {
                        listPeriode = await conn.QueryAsync<int>(
                            sql: $@"
                            SELECT periode
                            FROM bayar{tahun}
                            WHERE DATE(`tglbayar`)<=@cutoff
                            GROUP BY periode",
                            param: new
                            {
                                cutoff = settings.Cutoff,
                            },
                            transaction: trans);
                    });

                    foreach (var periode in listPeriode)
                    {
                        await Utils.TrackProgress($"bayar{tahun}-{periode}|rekening_air", async () =>
                        {
                            var lastId = 0;
                            await Utils.MainConnectionWrapper(async (conn, trans) =>
                            {
                                lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                            });

                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.LoketConnectionString,
                                targetConnection: AppSettings.MainConnectionString,
                                table: "rekening_air",
                                queryPath: @"Queries\bayar\bayar.sql",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                    { "@lastid", lastId },
                                    { "@periode", periode },
                                    { "@cutoff", settings.Cutoff },
                                },
                                placeholders: new()
                                {
                                    { "[table]", $"bayar{tahun}" },
                                    { "[bacameter]", AppSettings.DatabaseBacameter },
                                    { "[bsbs]", AppSettings.DatabaseBsbs },
                                });
                        });

                        await Utils.TrackProgress($"bayar{tahun}-{periode}|rekening_air_detail", async () =>
                        {
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.LoketConnectionString,
                                targetConnection: AppSettings.MainConnectionString,
                                table: "rekening_air_detail",
                                queryPath: @"Queries\bayar\bayar_detail.sql",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                    { "@periode", periode },
                                    { "@cutoff", settings.Cutoff },
                                },
                                placeholders: new()
                                {
                                    { "[table]", $"bayar{tahun}" },
                                    { "[bsbs]", AppSettings.DatabaseBsbs },
                                });
                        });

                        await Utils.TrackProgress($"bayar{tahun}-{periode}|rekening_air_transaksi", async () =>
                        {
                            await Utils.BulkCopy(
                                sourceConnection: AppSettings.LoketConnectionString,
                                targetConnection: AppSettings.MainConnectionString,
                                table: "rekening_air_transaksi",
                                queryPath: @"Queries\bayar\bayar_transaksi.sql",
                                parameters: new()
                                {
                                    { "@idpdam", settings.IdPdam },
                                    { "@periode", periode },
                                    { "@cutoff", settings.Cutoff },
                                },
                                placeholders: new()
                                {
                                    { "[table]", $"bayar{tahun}" },
                                    { "[bacameter]", AppSettings.DatabaseBacameter },
                                    { "[bsbs]", AppSettings.DatabaseBsbs },
                                });
                        });
                    }
                });
            }

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "rekening_air_transaksi",
                queryPath: @"Queries\bayar\bayar_transaksi_batal.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@cutoff", settings.Cutoff },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });
        }
        private static async Task Bayar(Settings settings)
        {
            await Utils.TrackProgress($"bayar|rekening_air", async () =>
            {
                var lastId = 0;
                await Utils.MainConnectionWrapper(async (conn, trans) =>
                {
                    lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                });

                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_air",
                    queryPath: @"Queries\bayar\bayar.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                        { "@lastid", lastId },
                    },
                    placeholders: new()
                    {
                        { "[bacameter]", AppSettings.DatabaseBacameter },
                        { "[bsbs]", AppSettings.DatabaseBsbs },
                        { "[dataawal]", AppSettings.DataAwalDatabase },
                    },
                    columnMappings: ColumnMappings["rekening_air"]);
            });

            await Utils.TrackProgress($"bayar|rekening_air_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_air_detail",
                    queryPath: @"Queries\bayar\bayar_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    },
                    placeholders: new()
                    {
                        { "[bacameter]", AppSettings.DatabaseBacameter },
                        { "[bsbs]", AppSettings.DatabaseBsbs },
                        { "[dataawal]", AppSettings.DataAwalDatabase },
                    },
                    columnMappings: ColumnMappings["rekening_air_detail"]);
            });

            await Utils.TrackProgress($"bayar|rekening_air_transaksi", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_air_transaksi",
                    queryPath: @"Queries\bayar\bayar_transaksi.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    },
                    placeholders: new()
                    {
                        { "[bacameter]", AppSettings.DatabaseBacameter },
                        { "[bsbs]", AppSettings.DatabaseBsbs },
                        { "[dataawal]", AppSettings.DataAwalDatabase },
                    },
                    columnMappings: ColumnMappings["rekening_air_transaksi"]);
            });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "rekening_air_transaksi",
                queryPath: @"Queries\bayar\bayar_transaksi_batal.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["rekening_air_transaksi"]);
        }
        private async Task RabLainnyaPelanggan(Settings settings)
        {
            var lastId = 0;
            IEnumerable<dynamic>? tipe = [];
            IEnumerable<dynamic>? baDetail = [];

            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                tipe = await conn.QueryAsync(
                    sql: @"
                    SELECT
                    a.idtipepermohonan,
                    a.idjenisnonair,
                    b.kodejenisnonair
                    FROM master_attribute_tipe_permohonan a
                    JOIN master_attribute_jenis_nonair b ON b.idpdam=a.idpdam AND b.idjenisnonair=a.idjenisnonair
                    WHERE a.idpdam=@idpdam AND a.flaghapus=0",
                    param: new
                    {
                        idpdam = settings.IdPdam
                    },
                    transaction: trans);
                baDetail = await conn.QueryAsync(
                    sql: @"
                    SELECT
                    `idtipepermohonan`,
                    `parameter`,
                    `tipedata`
                    FROM `master_attribute_tipe_permohonan_detail_ba` WHERE idpdam=@idpdam",
                    param: new
                    {
                        idpdam = settings.IdPdam
                    },
                    transaction: trans);

                if (tipe != null)
                {
                    await conn.ExecuteAsync(
                        sql: @"
                        DELETE FROM permohonan_pelanggan_air_spk_pasang WHERE idpdam=@idpdam AND `idpermohonan`
                        IN (SELECT idpermohonan FROM `permohonan_pelanggan_air_rab` WHERE idpdam=@idpdam AND flagrablainnya=1);

                        DELETE FROM permohonan_pelanggan_air_spk_pasang_detail WHERE idpdam=@idpdam AND `idpermohonan`
                        IN (SELECT idpermohonan FROM `permohonan_pelanggan_air_rab` WHERE idpdam=@idpdam AND flagrablainnya=1);

                        DELETE FROM permohonan_pelanggan_air_ba WHERE idpdam=@idpdam AND `idpermohonan` 
                        IN (SELECT idpermohonan FROM `permohonan_pelanggan_air_rab` WHERE idpdam=@idpdam AND flagrablainnya=1);

                        DELETE FROM permohonan_pelanggan_air_ba_detail WHERE idpdam=@idpdam AND `idpermohonan` 
                        IN (SELECT idpermohonan FROM `permohonan_pelanggan_air_rab` WHERE idpdam=@idpdam AND flagrablainnya=1);

                        DELETE FROM permohonan_pelanggan_air_detail WHERE idpdam=@idpdam AND `idpermohonan` 
                        IN (SELECT idpermohonan FROM `permohonan_pelanggan_air_rab` WHERE idpdam=@idpdam AND flagrablainnya=1);

                        DELETE FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idpermohonan` 
                        IN (SELECT idpermohonan FROM `permohonan_pelanggan_air_rab` WHERE idpdam=@idpdam AND flagrablainnya=1);

                        DELETE FROM `permohonan_pelanggan_air_rab_detail` WHERE idpdam=@idpdam AND `idpermohonan` 
                        IN (SELECT idpermohonan FROM `permohonan_pelanggan_air_rab` WHERE idpdam=@idpdam AND flagrablainnya=1);

                        DELETE FROM `permohonan_pelanggan_air_rab` WHERE idpdam=@idpdam AND flagrablainnya=1;",
                        param: new
                        {
                            idpdam = settings.IdPdam,
                            tipepermohonan = tipe.Select(s => s.idtipepermohonan).ToList()
                        },
                        transaction: trans);
                }

                lastId = await conn.QueryFirstOrDefaultAsync<int>(sql: @"SELECT IFNULL(MAX(idpermohonan),0) FROM permohonan_pelanggan_air", transaction: trans);
            });

            await Utils.LoketConnectionWrapper(async (conn, trans) =>
            {
                if (tipe != null)
                {
                    await conn.ExecuteAsync(
                        sql: @"
                        DROP TABLE IF EXISTS __tmp_tipepermohonan;

                        CREATE TABLE __tmp_tipepermohonan (
                        idtipepermohonan INT,
                        idjenisnonair INT,
                        kodejenisnonair VARCHAR(50))",
                        transaction: trans);

                    await conn.ExecuteAsync(
                        sql: @"INSERT INTO __tmp_tipepermohonan VALUES (@idtipepermohonan,@idjenisnonair,@kodejenisnonair)",
                        param: tipe,
                        transaction: trans);
                }

                if (baDetail != null)
                {
                    await conn.ExecuteAsync(
                        sql: @"
                        DROP TABLE IF EXISTS __tmp_badetail;

                        CREATE TABLE __tmp_badetail (
                        idtipepermohonan INT,
                        parameter VARCHAR(50),
                        tipedata VARCHAR(50))",
                        transaction: trans);

                    await conn.ExecuteAsync(
                        sql: @"INSERT INTO __tmp_badetail VALUES (@idtipepermohonan,@parameter,@tipedata)",
                        param: tipe,
                        transaction: trans);
                }
            });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air",
                queryPath: @"Queries\rab_lainnya_pelanggan\rab_lainnya.sql",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_rab",
                queryPath: @"Queries\rab_lainnya_pelanggan\rab.sql",
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

            //await Utils.BulkCopy(
            //    sConnectionStr: AppSettings.ConnectionStringLoket,
            //    tConnectionStr: AppSettings.ConnectionString,
            //    tableName: "permohonan_pelanggan_air_rab_detail",
            //    queryPath: @"Queries\sambung_kembali\rab_detail.sql",
            //    parameters: new()
            //    {
            //        { "@idpdam", settings.IdPdam },
            //        { "@lastid", lastId },
            //        { "@lastidrabdetail", rabdetail },
            //    });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_spk_pasang",
                queryPath: @"Queries\rab_lainnya_pelanggan\spk_pasang.sql",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\rab_lainnya_pelanggan\ba.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba_detail",
                queryPath: @"Queries\sambung_kembali\ba_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                });

            await Utils.LoketConnectionWrapper(async (conn, trans) =>
            {
                await conn.ExecuteAsync(sql: @"DROP TABLE IF EXISTS __tmp_jenisnonair", transaction: trans);
                await conn.ExecuteAsync(sql: @"DROP TABLE IF EXISTS __tmp_badetail", transaction: trans);
            });
        }
        private async Task AirTangkiPelanggan(Settings settings)
        {
            var lastId = 0;
            var tipe = 0;

            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                tipe = await conn.QueryFirstOrDefaultAsync<int>(
                    sql: $@"SELECT idtipepermohonan FROM master_attribute_tipe_permohonan WHERE idpdam={settings.IdPdam} AND kodetipepermohonan='AIR_TANGKI'",
                    transaction: trans);

                await conn.ExecuteAsync(
                    sql: @"
                    DELETE FROM permohonan_pelanggan_air_spk_pasang WHERE idpdam=@idpdam AND `idpermohonan`
                    IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@tipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_spk_pasang_detail WHERE idpdam=@idpdam AND `idpermohonan`
                    IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@tipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_ba WHERE idpdam=@idpdam AND `idpermohonan` 
                    IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@tipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_ba_detail WHERE idpdam=@idpdam AND `idpermohonan` 
                    IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@tipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_detail WHERE idpdam=@idpdam AND `idpermohonan` 
                    IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@tipepermohonan);

                    DELETE FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@tipepermohonan;",
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        tipepermohonan = tipe
                    },
                    transaction: trans);

                lastId = await conn.QueryFirstOrDefaultAsync<int>(sql: @"SELECT IFNULL(MAX(idpermohonan),0) FROM permohonan_pelanggan_air", transaction: trans);
            });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air",
                queryPath: @"Queries\air_tangki_pelanggan\pengaduan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                    { "@tipepermohonan", tipe },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_detail",
                queryPath: @"Queries\air_tangki_pelanggan\detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_spk_pasang",
                queryPath: @"Queries\air_tangki_pelanggan\spk_pasang.sql",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\air_tangki_pelanggan\ba.sql",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba_detail",
                queryPath: @"Queries\air_tangki_pelanggan\ba_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                });
        }
        private async Task AirTangkiNonPelanggan(Settings settings)
        {
            var lastId = 0;
            var tipe = 0;

            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                tipe = await conn.QueryFirstOrDefaultAsync<int>(
                    sql: $@"SELECT idtipepermohonan FROM master_attribute_tipe_permohonan WHERE idpdam={settings.IdPdam} AND kodetipepermohonan='AIR_TANGKI'",
                    transaction: trans);

                await conn.ExecuteAsync(
                    sql: @"
                    DELETE FROM permohonan_non_pelanggan_spk_pasang WHERE idpdam=@idpdam AND `idpermohonan`
                    IN (SELECT `idpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan`=@tipepermohonan);

                    DELETE FROM permohonan_non_pelanggan_spk_pasang_detail WHERE idpdam=@idpdam AND `idpermohonan`
                    IN (SELECT `idpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan`=@tipepermohonan);

                    DELETE FROM permohonan_non_pelanggan_ba WHERE idpdam=@idpdam AND `idpermohonan` 
                    IN (SELECT `idpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan`=@tipepermohonan);

                    DELETE FROM permohonan_non_pelanggan_ba_detail WHERE idpdam=@idpdam AND `idpermohonan` 
                    IN (SELECT `idpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan`=@tipepermohonan);

                    DELETE FROM permohonan_non_pelanggan_detail WHERE idpdam=@idpdam AND `idpermohonan` 
                    IN (SELECT `idpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan`=@tipepermohonan);

                    DELETE FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan`=@tipepermohonan;",
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        tipepermohonan = tipe
                    },
                    transaction: trans);

                lastId = await conn.QueryFirstOrDefaultAsync<int>(sql: @"SELECT IFNULL(MAX(idpermohonan),0) FROM permohonan_non_pelanggan", transaction: trans);
            });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan",
                queryPath: @"Queries\air_tangki_non_pelanggan\pengaduan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                    { "@tipepermohonan", tipe },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_detail",
                queryPath: @"Queries\air_tangki_non_pelanggan\detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_spk_pasang",
                queryPath: @"Queries\air_tangki_non_pelanggan\spk_pasang.sql",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_ba",
                queryPath: @"Queries\air_tangki_non_pelanggan\ba.sql",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_ba_detail",
                queryPath: @"Queries\air_tangki_non_pelanggan\ba_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                });
        }
        private async Task AngsuranNonair(Settings settings)
        {
            IEnumerable<dynamic>? jenis = [];
            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                await conn.ExecuteAsync(sql:
                    @"
                    TRUNCATE TABLE `rekening_nonair_angsuran`;
                    TRUNCATE TABLE `rekening_nonair_angsuran_detail`;",
                    transaction: trans);
                jenis = await conn.QueryAsync(
                    sql: @"SELECT idjenisnonair,kodejenisnonair FROM master_attribute_jenis_nonair WHERE idpdam=@idpdam AND flaghapus=0",
                    param: new
                    {
                        idpdam = settings.IdPdam
                    },
                    transaction: trans);
            });

            await Utils.LoketConnectionWrapper(async (conn, trans) =>
            {
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
                        VALUES (@idjenisnonair,@kodejenisnonair)",
                        param: jenis,
                        transaction: trans);
                }
            });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "rekening_nonair",
                queryPath: @"Queries\angsuran_nonair\nonair.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@cutoff", settings.Cutoff },
                },
                placeholders: new()
                {
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "rekening_nonair_detail",
                queryPath: @"Queries\angsuran_nonair\nonair_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@cutoff", settings.Cutoff },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "rekening_nonair_angsuran",
                queryPath: @"Queries\angsuran_nonair\nonair_angsuran.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@cutoff", settings.Cutoff },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "rekening_nonair_angsuran_detail",
                queryPath: @"Queries\angsuran_nonair\nonair_angsuran_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@cutoff", settings.Cutoff },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.TrackProgress($"angsuran nonair|patch0", async () =>
            {
                await Utils.MainConnectionWrapper(async (conn, trans) =>
                {
                    await conn.ExecuteAsync(
                        sql: await File.ReadAllTextAsync(@"Queries\nonair\patch.sql"),
                        transaction: trans,
                        commandTimeout: (int)TimeSpan.FromMinutes(15).TotalSeconds);
                });
            });

            await Utils.TrackProgress("angsuran nonair|patch1", async () =>
            {
                await Utils.MainConnectionWrapper(async (conn, trans) =>
                {
                    await conn.ExecuteAsync(
                        sql: await File.ReadAllTextAsync(@"Queries\angsuran_nonair\patch.sql"),
                        transaction: trans,
                        commandTimeout: (int)TimeSpan.FromMinutes(15).TotalSeconds);
                });
            });

            await Utils.LoketConnectionWrapper(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"
                    DROP TABLE IF EXISTS __tmp_jenisnonair;",
                    transaction: trans);
            });
        }
        private async Task AngsuranAir(Settings settings)
        {
            await Utils.TrackProgress($"angsuran air piutang|rekening_air", async () =>
            {
                var lastId = 0;
                await Utils.MainConnectionWrapper(async (conn, trans) =>
                {
                    await conn.ExecuteAsync(
                        sql: @"
                        TRUNCATE TABLE `rekening_air_angsuran`;
                        TRUNCATE TABLE `rekening_air_angsuran_detail`;",
                        transaction: trans);

                    lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                });

                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_air",
                    queryPath: @"Queries\angsuran_air\piutang_rekening_air.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                        { "@lastid", lastId },
                        { "@cutoff", settings.Cutoff },
                    },
                    placeholders: new()
                    {
                        { "[bacameter]", AppSettings.DatabaseBacameter },
                        { "[bsbs]", AppSettings.DatabaseBsbs },
                    });
            });

            await Utils.TrackProgress($"angsuran air piutang|rekening_air_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_air_detail",
                    queryPath: @"Queries\angsuran_air\piutang_rekening_air_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                        { "@cutoff", settings.Cutoff },
                    },
                    placeholders: new()
                    {
                        { "[bsbs]", AppSettings.DatabaseBsbs },
                    });
            });

            await Utils.TrackProgress($"angsuran air bayar|rekening_air", async () =>
            {
                var lastId = 0;
                await Utils.MainConnectionWrapper(async (conn, trans) =>
                {
                    lastId = await conn.QueryFirstOrDefaultAsync<int>("SELECT IFNULL(MAX(idrekeningair),0) FROM rekening_air", transaction: trans);
                });

                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_air",
                    queryPath: @"Queries\angsuran_air\bayar_rekening_air.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                        { "@lastid", lastId },
                        { "@cutoff", settings.Cutoff },
                    },
                    placeholders: new()
                    {
                        { "[bacameter]", AppSettings.DatabaseBacameter },
                        { "[bsbs]", AppSettings.DatabaseBsbs },
                    });
            });

            await Utils.TrackProgress($"angsuran air bayar|rekening_air_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_air_detail",
                    queryPath: @"Queries\angsuran_air\bayar_rekening_air_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                        { "@cutoff", settings.Cutoff },
                    },
                    placeholders: new()
                    {
                        { "[bsbs]", AppSettings.DatabaseBsbs },
                    });
            });

            await Utils.TrackProgress($"angsuran air|rekening_air_angsuran", async () =>
            {
                var jnsNonair = 0;
                await Utils.MainConnectionWrapper(async (conn, trans) =>
                {
                    jnsNonair = await conn.QueryFirstOrDefaultAsync<int>($"SELECT idjenisnonair FROM master_attribute_jenis_nonair WHERE idpdam = {settings.IdPdam} AND kodejenisnonair = 'JNS-36' AND flaghapus = 0", transaction: trans);
                });

                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_air_angsuran",
                    queryPath: @"Queries\angsuran_air\rekening_air_angsuran.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                        { "@jnsnonair", jnsNonair },
                        { "@cutoff", settings.Cutoff },
                    },
                    placeholders: new()
                    {
                        { "[bacameter]", AppSettings.DatabaseBacameter },
                        { "[bsbs]", AppSettings.DatabaseBsbs },
                    });
            });

            await Utils.TrackProgress($"angsuran air|rekening_air_angsuran_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_air_angsuran_detail",
                    queryPath: @"Queries\angsuran_air\rekening_air_angsuran_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                        { "@cutoff", settings.Cutoff },
                    },
                    placeholders: new()
                    {
                        { "[bacameter]", AppSettings.DatabaseBacameter },
                        { "[bsbs]", AppSettings.DatabaseBsbs },
                    });
            });

            await Utils.TrackProgress("angsuran air|patch", async () =>
            {
                await Utils.MainConnectionWrapper(async (conn, trans) =>
                {
                    await conn.ExecuteAsync(
                        sql: await File.ReadAllTextAsync(@"Queries\angsuran_air\patch.sql"),
                        transaction: trans,
                        commandTimeout: (int)TimeSpan.FromMinutes(15).TotalSeconds);
                });
            });
        }
        private static async Task KoreksiData(Settings settings)
        {
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_pelanggan_air_riwayat_koreksi",
                queryPath: @"Queries\koreksi_data\koreksi_data.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["master_pelanggan_air_riwayat_koreksi"]);

            //copy terbaru
            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "tampung_koreksi_data",
                query: @"SELECT idpdam,`idkoreksi`,nomor FROM `master_pelanggan_air_riwayat_koreksi` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            var lastIdKoreksiDetail = 0;
            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                var idPermohonanList = await conn.QueryAsync<int>(
                    sql: @"select idkoreksi from master_pelanggan_air_riwayat_koreksi where idpdam=@idpdam",
                    param: new
                    {
                        idpdam = settings.IdPdam,
                    },
                    transaction: trans);
                if (idPermohonanList.Any())
                {
                    await conn.ExecuteAsync(
                        sql: @"delete from master_pelanggan_air_riwayat_koreksi_detail where idpdam=@idpdam and idkoreksi in @idkoreksi",
                        param: new
                        {
                            idpdam = settings.IdPdam,
                            idkoreksi = idPermohonanList.ToList(),
                        },
                        transaction: trans);
                }
                lastIdKoreksiDetail = await conn.QueryFirstOrDefaultAsync<int>(
                    sql: @"SELECT COALESCE(MAX(`id`),0) AS maxid FROM `master_pelanggan_air_riwayat_koreksi_detail`",
                    transaction: trans);
            });
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_pelanggan_air_riwayat_koreksi_detail",
                queryPath: @"Queries\koreksi_data\koreksi_data_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastIdKoreksiDetail },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["master_pelanggan_air_riwayat_koreksi_detail"]);
        }
        private async Task Report(Settings settings)
        {
            await Utils.TrackProgress("master_attribute_label_report", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "master_attribute_label_report",
                    queryPath: @"Queries\master\report\master_attribute_label_report.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_report_maingroup", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "master_report_maingroup",
                    queryPath: @"Queries\master\report\master_report_maingroup.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_report_subgroup", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "master_report_subgroup",
                    queryPath: @"Queries\master\report\master_report_subgroup.sql");
            });

            await Utils.TrackProgress("report_api", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "report_api",
                    queryPath: @"Queries\master\report\report_api.sql");
            });

            await Utils.TrackProgress("report_models", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "report_models",
                    queryPath: @"Queries\master\report\report_models.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("report_model_sources", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "report_model_sources",
                    queryPath: @"Queries\master\report\report_model_sources.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("report_model_sorts", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "report_model_sorts",
                    queryPath: @"Queries\master\report\report_model_sorts.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("report_model_props", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "report_model_props",
                    queryPath: @"Queries\master\report\report_model_props.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("report_model_params", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "report_model_params",
                    queryPath: @"Queries\master\report\report_model_params.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("report_filter_custom", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "report_filter_custom",
                    queryPath: @"Queries\master\report\report_filter_custom.sql");
            });

            await Utils.TrackProgress("report_filter_custom_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "report_filter_custom_detail",
                    queryPath: @"Queries\master\report\report_filter_custom_detail.sql");
            });
        }
        private static async Task PaketRab(Settings settings)
        {
            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"
                    truncate table master_attribute_paket;",
                    transaction: trans);
            });

            await Utils.TrackProgress("master_attribute_paket", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "master_attribute_paket",
                    queryPath: @"Queries\master\master_attribute_paket.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });
        }
        private static async Task PaketOngkos(Settings settings)
        {
            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"
                    truncate table master_attribute_ongkos;
                    truncate table master_attribute_ongkos_paket;
                    truncate table master_attribute_ongkos_paket_detail;",
                    transaction: trans);
            });

            await Utils.TrackProgress("master_attribute_ongkos", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "master_attribute_ongkos",
                    queryPath: @"Queries\master\paket_ongkos\master_attribute_ongkos.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_ongkos_paket", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "master_attribute_ongkos_paket",
                    queryPath: @"Queries\master\paket_ongkos\master_attribute_ongkos_paket.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_ongkos_paket_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "master_attribute_ongkos_paket_detail",
                    queryPath: @"Queries\master\paket_ongkos\master_attribute_ongkos_paket_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });
        }
        private static async Task PaketMaterial(Settings settings)
        {
            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"
                    truncate table master_attribute_material;
                    truncate table master_attribute_material_paket;
                    truncate table master_attribute_material_paket_detail;",
                    transaction: trans);
            });

            await Utils.TrackProgress("master_attribute_material", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "master_attribute_material",
                    queryPath: @"Queries\master\paket_material\master_attribute_material.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_material_paket", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "master_attribute_material_paket",
                    queryPath: @"Queries\master\paket_material\master_attribute_material_paket.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_material_paket_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "master_attribute_material_paket_detail",
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
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "master_attribute_tipe_permohonan",
                    queryPath: @"Queries\master\tipe_permohonan\master_attribute_tipe_permohonan.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_tipe_permohonan_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "master_attribute_tipe_permohonan_detail",
                    queryPath: @"Queries\master\tipe_permohonan\master_attribute_tipe_permohonan_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_tipe_permohonan_detail_ba", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "master_attribute_tipe_permohonan_detail_ba",
                    queryPath: @"Queries\master\tipe_permohonan\master_attribute_tipe_permohonan_detail_ba.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_tipe_permohonan_detail_spk", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "master_attribute_tipe_permohonan_detail_spk",
                    queryPath: @"Queries\master\tipe_permohonan\master_attribute_tipe_permohonan_detail_spk.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });
        }
        private static async Task MasterData(Settings settings)
        {
            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                queryPath: @"Queries\master\master_attribute_flag.sql",
                table: "master_attribute_flag",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                columnMappings:
                [
                    new(0, "idpdam"),
                    new(1, "idflag"),
                    new(2, "namaflag"),
                    new(3, "flaghapus"),
                    new(4, "waktuupdate"),
                ]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_status",
                queryPath: @"Queries\master\master_attribute_status.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                columnMappings:
                [
                    new(0, "idpdam"),
                    new(1, "idstatus"),
                    new(2, "namastatus"),
                    new(3, "rekening_air_include"),
                    new(4, "rekening_limbah_include"),
                    new(5, "rekening_lltt_include"),
                    new(6, "tanpabiayapemakaianair"),
                    new(7, "flaghapus"),
                    new(8, "waktuupdate"),
                ]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_jenis_bangunan",
                queryPath: @"Queries\master\master_attribute_jenis_bangunan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_kepemilikan",
                queryPath: @"Queries\master\master_attribute_kepemilikan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_pekerjaan",
                queryPath: @"Queries\master\master_attribute_pekerjaan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_peruntukan",
                queryPath: @"Queries\master\master_attribute_peruntukan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_jenis_pipa",
                queryPath: @"Queries\master\master_attribute_jenis_pipa.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_kwh",
                queryPath: @"Queries\master\master_attribute_kwh.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_tarif_golongan",
                queryPath: @"Queries\master\master_tarif_golongan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_tarif_golongan_detail",
                queryPath: @"Queries\master\master_tarif_golongan_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_tarif_diameter",
                queryPath: @"Queries\master\master_tarif_diameter.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_tarif_diameter_detail",
                queryPath: @"Queries\master\master_tarif_diameter_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_wilayah",
                queryPath: @"Queries\master\master_attribute_wilayah.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_area",
                queryPath: @"Queries\master\master_attribute_area.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_rayon",
                queryPath: @"Queries\master\master_attribute_rayon.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_blok",
                queryPath: @"Queries\master\master_attribute_blok.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_cabang",
                queryPath: @"Queries\master\master_attribute_cabang.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_kecamatan",
                queryPath: @"Queries\master\master_attribute_kecamatan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_kelurahan",
                queryPath: @"Queries\master\master_attribute_kelurahan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_dma",
                queryPath: @"Queries\master\master_attribute_dma.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_dmz",
                queryPath: @"Queries\master\master_attribute_dmz.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_tarif_administrasi_lain",
                queryPath: @"Queries\master\master_tarif_administrasi_lain.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_tarif_pemeliharaan_lain",
                queryPath: @"Queries\master\master_tarif_pemeliharaan_lain.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_tarif_retribusi_lain",
                queryPath: @"Queries\master\master_tarif_retribusi_lain.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"
                    ALTER TABLE master_attribute_kolektif
                    CHANGE kodekolektif kodekolektif VARCHAR (50) CHARSET latin1 COLLATE latin1_swedish_ci NOT NULL",
                    transaction: trans);
            });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_kolektif",
                queryPath: @"Queries\master\master_attribute_kolektif.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_sumber_air",
                queryPath: @"Queries\master\master_attribute_sumber_air.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_merek_meter",
                queryPath: @"Queries\master\master_attribute_merek_meter.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"
                    ALTER TABLE master_attribute_kondisi_meter
                    CHANGE kodekondisimeter kodekondisimeter VARCHAR (50) CHARSET latin1 COLLATE latin1_swedish_ci NOT NULL",
                    transaction: trans);
            });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_kondisi_meter",
                queryPath: @"Queries\master\master_attribute_kondisi_meter.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_kelainan",
                queryPath: @"Queries\master\master_attribute_kelainan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_petugas_baca",
                queryPath: @"Queries\master\master_attribute_petugas_baca.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_periode",
                queryPath: @"Queries\master\master_periode.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_periode_billing",
                queryPath: @"Queries\master\master_periode_billing.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BacameterConnectionWrapper(async (conn, trans) =>
            {
                var jadwalbaca = await conn.QueryAsync(
                    sql: @"
                    SELECT
                    b.kodepetugas,
                    c.koderayon
                    FROM jadwalbaca a
                    JOIN petugasbaca b ON a.idpetugas=b.idpetugas
                    JOIN rayon c ON a.idrayon=c.idrayon",
                    transaction: trans);
                if (jadwalbaca.Any())
                {
                    await Utils.MainConnectionWrapper(async (conn, trans) =>
                    {
                        List<dynamic> data = [];
                        var listPetugas = await conn.QueryAsync(
                            sql: @"SELECT idpetugasbaca,kodepetugasbaca FROM master_attribute_petugas_baca WHERE idpdam=@idpdam",
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
                                        .Where(s => s.kodepetugasbaca.ToLower() == item.kodepetugas.ToLower())
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
                                sql: @"DELETE FROM master_attribute_jadwal_baca WHERE idpdam=@idpdam",
                                param: new
                                {
                                    idpdam = settings.IdPdam
                                },
                                transaction: trans);
                            await conn.ExecuteAsync(
                                sql: @"INSERT INTO master_attribute_jadwal_baca (idpdam,idjadwalbaca,idpetugasbaca,idrayon)
                                VALUES (@idpdam,@idjadwalbaca,@idpetugasbaca,@idrayon)",
                                param: data,
                                transaction: trans);
                        }
                    });
                }
            });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_loket",
                queryPath: @"Queries\master\master_attribute_loket.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_user",
                queryPath: @"Queries\master\master_user.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBacameter },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                    { "[loket]", AppSettings.LoketDatabase },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_tipe_pendaftaran_sambungan",
                queryPath: @"Queries\master\master_attribute_tipe_pendaftaran_sambungan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_sumber_pengaduan",
                queryPath: @"Queries\master\master_attribute_sumber_pengaduan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "personalia_master_pegawai",
                queryPath: @"Queries\master\personalia_master_pegawai.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                columnMappings:
                [
                    new(0, "idpdam"),
                    new(1, "idpegawai"),
                    new(2, "nomorindukpegawai"),
                    new(3, "namapegawai"),
                    new(4, "alamatktp"),
                    new(5, "flagaktif"),
                    new(6, "flaghapus"),
                    new(7, "waktuupdate"),
                ]);
        }
        private async Task JenisNonair(Settings settings)
        {
            await Utils.TrackProgress("master_attribute_jenis_nonair", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "master_attribute_jenis_nonair",
                    queryPath: @"Queries\master\jenis_nonair\master_attribute_jenis_nonair.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("master_attribute_jenis_nonair_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.ConnectionStringStaging,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "master_attribute_jenis_nonair_detail",
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
            await Utils.MainConnectionWrapper(async (conn, trans) =>
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

            await Utils.LoketConnectionWrapper(async (conn, trans) =>
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_spk",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_rab",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_rab_detail",
                queryPath: @"Queries\rotasimeter_nonrutin\rabdetail_rotasimeter_nonrutin.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", rabdetail },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_spk_pasang",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba",
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

            await Utils.LoketConnectionWrapper(async (conn, trans) =>
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
            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                rotasimeter = await conn.QueryFirstOrDefaultAsync<int>($@"SELECT idtipepermohonan FROM master_attribute_tipe_permohonan WHERE idpdam={settings.IdPdam} AND kodetipepermohonan='GANTI_METER_RUTIN' AND flaghapus=0", transaction: trans);
                await conn.ExecuteAsync(
                    sql: @"
                    DELETE FROM permohonan_pelanggan_air_spk_pasang WHERE idpdam=@idpdam AND `idpermohonan`
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_spk_pasang_detail WHERE idpdam=@idpdam AND `idpermohonan`
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_ba WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_ba_detail WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM permohonan_pelanggan_air_detail WHERE idpdam=@idpdam AND `idpermohonan` 
                     IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan);

                    DELETE FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan`=@idtipepermohonan;",
                    param: new
                    {
                        idpdam = settings.IdPdam,
                        idtipepermohonan = rotasimeter
                    },
                    transaction: trans);
                lastId = await conn.QueryFirstOrDefaultAsync<int>(@"SELECT IFNULL(MAX(idpermohonan),0) FROM permohonan_pelanggan_air", transaction: trans);
            });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air",
                queryPath: @"Queries\rotasimeter\rotasimeter.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                    { "@tipepermohonan", rotasimeter },
                },
                placeholders: new()
                {
                    { "[bacameter]", AppSettings.DatabaseBsbs },
                    { "[bsbs]", AppSettings.DatabaseBsbs },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_spk_pasang",
                queryPath: @"Queries\rotasimeter\spk_pasang.sql",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\rotasimeter\ba.sql",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba_detail",
                queryPath: @"Queries\rotasimeter\ba_detail.sql",
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

            await Utils.LoketConnectionWrapper(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: @"
                    DROP TABLE IF EXISTS __tmp_rotasimeter",
                    transaction: trans);
            });
        }
        private static async Task KoreksiRekair(Settings settings)
        {
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air",
                queryPath: @"Queries\koreksi_rekair\koreksi_rekair.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawaal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air"]);

            //copy terbaru
            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "tampung_permohonan_pelanggan_air",
                query: @"SELECT idpdam,`idpermohonan`,idtipepermohonan,`nomorpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            var lastId = 0;
            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                var idPermohonanList = await conn.QueryAsync<int>(
                    sql: @"
                    SELECT a.`idpermohonan` FROM permohonan_non_pelanggan a
                    JOIN `master_attribute_tipe_permohonan` b ON b.`idpdam`=a.`idpdam` AND b.`idtipepermohonan`=a.`idtipepermohonan`
                    WHERE a.`idpdam`=@idpdam AND b.`kodetipepermohonan`='KREKAIR'",
                    param: new
                    {
                        idpdam = settings.IdPdam,
                    },
                    transaction: trans);
                if (idPermohonanList.Any())
                {
                    await conn.ExecuteAsync(
                        sql: @"delete from permohonan_pelanggan_air_koreksi_rekening where idpdam=@idpdam and idpermohonan in @idpermohonan",
                        param: new
                        {
                            idpdam = settings.IdPdam,
                            idpermohonan = idPermohonanList.ToList(),
                        },
                        transaction: trans);
                }
                lastId = await conn.QueryFirstOrDefaultAsync<int>(
                    sql: @"SELECT COALESCE(MAX(`id`),0) AS maxid FROM `permohonan_pelanggan_air_koreksi_rekening`",
                    transaction: trans);
            });
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_koreksi_rekening",
                queryPath: @"Queries\koreksi_rekair\koreksi_rekair_periode.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_koreksi_rekening"]);
        }
        private static async Task SambungBaru(Settings settings)
        {
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan",
                queryPath: @"Queries\sambung_baru\sambung_baru.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_non_pelanggan"]);

            //copy lg supaya dapet yg terbaru
            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "tampung_permohonan_non_pelanggan",
                query: @"SELECT idpdam,`idpermohonan`,idtipepermohonan,`nomorpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_detail",
                queryPath: @"Queries\sambung_baru\detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_non_pelanggan_detail"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_spk",
                queryPath: @"Queries\sambung_baru\spk.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_non_pelanggan_spk"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_spk_detail",
                queryPath: @"Queries\sambung_baru\spk_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_non_pelanggan_spk_detail"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_rab",
                queryPath: @"Queries\sambung_baru\rab.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_non_pelanggan_rab"]);

            var lastIdRabDetail = 0;
            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                var idPermohonanList = await conn.QueryAsync<int>(
                    sql: @"
                    SELECT a.`idpermohonan` FROM permohonan_non_pelanggan a
                    JOIN `master_attribute_tipe_permohonan` b ON b.`idpdam`=a.`idpdam` AND b.`idtipepermohonan`=a.`idtipepermohonan`
                    WHERE a.`idpdam`=@idpdam AND b.`kodetipepermohonan`='SAMBUNGAN_BARU_AIR'",
                    param: new
                    {
                        idpdam = settings.IdPdam,
                    },
                    transaction: trans);
                if (idPermohonanList.Any())
                {
                    await conn.ExecuteAsync(
                        sql: @"delete from permohonan_non_pelanggan_rab_detail where idpdam=@idpdam and idpermohonan in @idpermohonan",
                        param: new
                        {
                            idpdam = settings.IdPdam,
                            idpermohonan = idPermohonanList.ToList(),
                        },
                        transaction: trans);
                }
                lastIdRabDetail = await conn.QueryFirstOrDefaultAsync<int>(
                    sql: @"SELECT COALESCE(MAX(`id`),0) AS maxid FROM `permohonan_non_pelanggan_rab_detail`",
                    transaction: trans);
            });
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_rab_detail",
                queryPath: @"Queries\sambung_baru\rab_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastIdRabDetail },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_non_pelanggan_rab_detail"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_spk_pasang",
                queryPath: @"Queries\sambung_baru\spk_pasang.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_non_pelanggan_spk_pasang"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_ba",
                queryPath: @"Queries\sambung_baru\ba.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_non_pelanggan_ba"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_ba_detail",
                queryPath: @"Queries\sambung_baru\ba_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_non_pelanggan_ba_detail"]);
        }
        private static async Task BukaSegel(Settings settings)
        {
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air",
                queryPath: @"Queries\buka_segel\buka_segel.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air"]);

            //copy terbaru
            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "tampung_permohonan_pelanggan_air",
                query: @"SELECT idpdam,`idpermohonan`,idtipepermohonan,`nomorpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_detail",
                queryPath: @"Queries\buka_segel\detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_detail"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_spk_pasang",
                queryPath: @"Queries\buka_segel\spk_pasang.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_spk_pasang"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\buka_segel\ba.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_ba"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba_detail",
                queryPath: @"Queries\buka_segel\ba_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_ba_detail"]);
        }
        private static async Task SambungKembali(Settings settings)
        {
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air",
                queryPath: @"Queries\sambung_kembali\sambung_kembali.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air"]);

            //copy lg supaya dapet yg terbaru
            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "tampung_permohonan_pelanggan_air",
                query: @"SELECT idpdam,`idpermohonan`,idtipepermohonan,`nomorpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_detail",
                queryPath: @"Queries\sambung_kembali\detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_detail"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_spk",
                queryPath: @"Queries\sambung_kembali\spk.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_spk"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_spk_detail",
                queryPath: @"Queries\sambung_kembali\spk_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_spk_detail"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_rab",
                queryPath: @"Queries\sambung_kembali\rab.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_rab"]);

            var lastIdRabDetail = 0;
            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                var idPermohonanList = await conn.QueryAsync<int>(
                    sql: @"
                    SELECT a.`idpermohonan` FROM permohonan_pelanggan_air a
                    JOIN `master_attribute_tipe_permohonan` b ON b.`idpdam`=a.`idpdam` AND b.`idtipepermohonan`=a.`idtipepermohonan`
                    WHERE a.`idpdam`=@idpdam AND b.`kodetipepermohonan`='SAMBUNG_KEMBALI'",
                    param: new
                    {
                        idpdam = settings.IdPdam,
                    },
                    transaction: trans);
                if (idPermohonanList.Any())
                {
                    await conn.ExecuteAsync(
                        sql: @"delete from permohonan_pelanggan_air_rab_detail where idpdam=@idpdam and idpermohonan in @idpermohonan",
                        param: new
                        {
                            idpdam = settings.IdPdam,
                            idpermohonan = idPermohonanList.ToList(),
                        },
                        transaction: trans);
                }
                lastIdRabDetail = await conn.QueryFirstOrDefaultAsync<int>(
                    sql: @"SELECT COALESCE(MAX(`id`),0) AS maxid FROM `permohonan_pelanggan_air_rab_detail`",
                    transaction: trans);
            });
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_rab_detail",
                queryPath: @"Queries\sambung_kembali\rab_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastIdRabDetail },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_rab_detail"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_spk_pasang",
                queryPath: @"Queries\sambung_kembali\spk_pasang.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_spk_pasang"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\sambung_kembali\ba.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_ba"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba_detail",
                queryPath: @"Queries\sambung_kembali\ba_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_ba_detail"]);
        }
        private static async Task RubahRayon(Settings settings)
        {
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air",
                queryPath: @"Queries\rubah_rayon\rubah_rayon.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air"]);

            //copy terbaru
            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "tampung_permohonan_pelanggan_air",
                query: @"SELECT idpdam,`idpermohonan`,idtipepermohonan,`nomorpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_detail",
                queryPath: @"Queries\rubah_rayon\detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_detail"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\rubah_rayon\ba.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_ba"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba_detail",
                queryPath: @"Queries\rubah_rayon\ba_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_ba_detail"]);
        }
        private static async Task RubahTarif(Settings settings)
        {
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air",
                queryPath: @"Queries\rubah_tarif\rubah_tarif.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air"]);

            //copy terbaru
            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "tampung_permohonan_pelanggan_air",
                query: @"SELECT idpdam,`idpermohonan`,idtipepermohonan,`nomorpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_detail",
                queryPath: @"Queries\rubah_tarif\detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_detail"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_spk",
                queryPath: @"Queries\rubah_tarif\spk.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_spk"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_spk_detail",
                queryPath: @"Queries\rubah_tarif\spk_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_spk_detail"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\rubah_tarif\ba.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_ba"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba_detail",
                queryPath: @"Queries\rubah_tarif\ba_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_ba_detail"]);
        }
        private static async Task BalikNama(Settings settings)
        {
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air",
                queryPath: @"Queries\balik_nama\balik_nama.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air"]);

            //copy terbaru
            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "tampung_permohonan_pelanggan_air",
                query: @"SELECT idpdam,`idpermohonan`,idtipepermohonan,`nomorpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_detail",
                queryPath: @"Queries\balik_nama\detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_detail"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\balik_nama\ba.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_ba"]);
        }
        private static async Task TutupTotal(Settings settings)
        {
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air",
                queryPath: @"Queries\tutup_total\tutup_total.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air"]);

            //copy terbaru
            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.DataAwalConnectionString,
                table: "tampung_permohonan_pelanggan_air",
                query: @"SELECT idpdam,`idpermohonan`,idtipepermohonan,`nomorpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_detail",
                queryPath: @"Queries\tutup_total\detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_detail"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_spk",
                queryPath: @"Queries\tutup_total\spk.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_spk"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_spk_pasang",
                queryPath: @"Queries\tutup_total\spk_pasang.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_spk_pasang"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\tutup_total\ba.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_ba"]);

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba_detail",
                queryPath: @"Queries\tutup_total\ba_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                },
                placeholders: new()
                {
                    { "[dataawal]", AppSettings.DataAwalDatabase },
                },
                columnMappings: ColumnMappings["permohonan_pelanggan_air_ba_detail"]);
        }
        private async Task PengaduanPelanggan(Settings settings)
        {
            var lastId = 0;
            IEnumerable<dynamic>? tipe = [];

            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                tipe = await conn.QueryAsync(
                    sql: @"
                    SELECT
                    a.idtipepermohonan,
                    a.idjenisnonair,
                    b.kodejenisnonair
                    FROM master_attribute_tipe_permohonan a
                    JOIN master_attribute_jenis_nonair b ON b.idpdam=a.idpdam AND b.idjenisnonair=a.idjenisnonair
                    WHERE a.idpdam=@idpdam AND a.flaghapus=0 AND a.flagaktif=1",
                    param: new
                    {
                        idpdam = settings.IdPdam
                    },
                    transaction: trans);

                if (tipe != null)
                {
                    await conn.ExecuteAsync(
                        sql: @"
                        DELETE FROM permohonan_pelanggan_air_spk_pasang WHERE idpdam=@idpdam AND `idpermohonan`
                        IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan` in @tipepermohonan);

                        DELETE FROM permohonan_pelanggan_air_spk_pasang_detail WHERE idpdam=@idpdam AND `idpermohonan`
                        IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan` in @tipepermohonan);

                        DELETE FROM permohonan_pelanggan_air_ba WHERE idpdam=@idpdam AND `idpermohonan` 
                        IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan` in @tipepermohonan);

                        DELETE FROM permohonan_pelanggan_air_ba_detail WHERE idpdam=@idpdam AND `idpermohonan` 
                        IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan` in @tipepermohonan);

                        DELETE FROM permohonan_pelanggan_air_detail WHERE idpdam=@idpdam AND `idpermohonan` 
                        IN (SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan` in @tipepermohonan);

                        DELETE FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan` in @tipepermohonan;",
                        param: new
                        {
                            idpdam = settings.IdPdam,
                            tipepermohonan = tipe.Select(s => s.idtipepermohonan).ToList()
                        },
                        transaction: trans);
                }

                lastId = await conn.QueryFirstOrDefaultAsync<int>(sql: @"SELECT IFNULL(MAX(idpermohonan),0) FROM permohonan_pelanggan_air", transaction: trans);
            });

            await Utils.LoketConnectionWrapper(async (conn, trans) =>
            {
                if (tipe != null)
                {
                    await conn.ExecuteAsync(
                        sql: @"
                        DROP TABLE IF EXISTS __tmp_tipepermohonan;

                        CREATE TABLE __tmp_tipepermohonan (
                        idtipepermohonan INT,
                        idjenisnonair INT,
                        kodejenisnonair VARCHAR(50))",
                        transaction: trans);

                    await conn.ExecuteAsync(
                        sql: @"INSERT INTO __tmp_tipepermohonan VALUES (@idtipepermohonan,@idjenisnonair,@kodejenisnonair)",
                        param: tipe,
                        transaction: trans);
                }
            });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air",
                queryPath: @"Queries\pengaduan_pelanggan\pengaduan.sql",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_detail",
                queryPath: @"Queries\pengaduan_pelanggan\detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_detail",
                queryPath: @"Queries\pengaduan_pelanggan\detail2.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_spk_pasang",
                queryPath: @"Queries\pengaduan_pelanggan\spk_pasang.sql",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba",
                queryPath: @"Queries\pengaduan_pelanggan\ba.sql",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba_detail",
                queryPath: @"Queries\pengaduan_pelanggan\ba_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba_detail",
                queryPath: @"Queries\pengaduan_pelanggan\ba_detail2.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba_detail",
                queryPath: @"Queries\pengaduan_pelanggan\ba_detail3.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba_detail",
                queryPath: @"Queries\pengaduan_pelanggan\ba_detail4.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                });

            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                await conn.ExecuteAsync(
                    sql: await File.ReadAllTextAsync(@"Queries\pengaduan_pelanggan\patches\p1.sql"),
                    param: new
                    {
                        idpdam = settings.IdPdam,
                    },
                    transaction: trans,
                    commandTimeout: (int)TimeSpan.FromHours(1).TotalSeconds);
            });

            await Utils.LoketConnectionWrapper(async (conn, trans) =>
            {
                await conn.ExecuteAsync(@"DROP TABLE IF EXISTS __tmp_tipepermohonan", transaction: trans);
            });
        }
        private async Task PengaduanNonPelanggan(Settings settings)
        {
            var lastId = 0;
            IEnumerable<dynamic>? tipe = [];

            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                tipe = await conn.QueryAsync(
                    sql: @"
                    SELECT
                    a.idtipepermohonan,
                    a.idjenisnonair,
                    b.kodejenisnonair
                    FROM master_attribute_tipe_permohonan a
                    JOIN master_attribute_jenis_nonair b ON b.idpdam=a.idpdam AND b.idjenisnonair=a.idjenisnonair
                    WHERE a.idpdam=@idpdam AND a.flaghapus=0 AND a.flagaktif=1",
                    param: new
                    {
                        idpdam = settings.IdPdam
                    },
                    transaction: trans);

                if (tipe != null)
                {
                    await conn.ExecuteAsync(
                        sql: @"
                        DELETE FROM permohonan_non_pelanggan_spk_pasang WHERE idpdam=@idpdam AND `idpermohonan`
                        IN (SELECT `idpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan` in @tipepermohonan);

                        DELETE FROM permohonan_non_pelanggan_spk_pasang_detail WHERE idpdam=@idpdam AND `idpermohonan`
                        IN (SELECT `idpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan` in @tipepermohonan);

                        DELETE FROM permohonan_non_pelanggan_ba WHERE idpdam=@idpdam AND `idpermohonan` 
                        IN (SELECT `idpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan` in @tipepermohonan);

                        DELETE FROM permohonan_non_pelanggan_ba_detail WHERE idpdam=@idpdam AND `idpermohonan` 
                        IN (SELECT `idpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan` in @tipepermohonan);

                        DELETE FROM permohonan_non_pelanggan_detail WHERE idpdam=@idpdam AND `idpermohonan` 
                        IN (SELECT `idpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan` in @tipepermohonan);

                        DELETE FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam AND `idtipepermohonan` in @tipepermohonan;",
                        param: new
                        {
                            idpdam = settings.IdPdam,
                            tipepermohonan = tipe.Select(s => s.idtipepermohonan).ToList()
                        },
                        transaction: trans);
                }

                lastId = await conn.QueryFirstOrDefaultAsync<int>(sql: @"SELECT IFNULL(MAX(idpermohonan),0) FROM permohonan_non_pelanggan", transaction: trans);
            });

            await Utils.LoketConnectionWrapper(async (conn, trans) =>
            {
                if (tipe != null)
                {
                    await conn.ExecuteAsync(
                        sql: @"
                        DROP TABLE IF EXISTS __tmp_tipepermohonan;

                        CREATE TABLE __tmp_tipepermohonan (
                        idtipepermohonan INT,
                        idjenisnonair INT,
                        kodejenisnonair VARCHAR(50))",
                        transaction: trans);

                    await conn.ExecuteAsync(
                        sql: @"INSERT INTO __tmp_tipepermohonan VALUES (@idtipepermohonan,@idjenisnonair,@kodejenisnonair)",
                        param: tipe,
                        transaction: trans);
                }
            });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan",
                queryPath: @"Queries\pengaduan_non_pelanggan\pengaduan.sql",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_detail",
                queryPath: @"Queries\pengaduan_non_pelanggan\detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_spk_pasang",
                queryPath: @"Queries\pengaduan_non_pelanggan\spk_pasang.sql",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_ba",
                queryPath: @"Queries\pengaduan_non_pelanggan\ba.sql",
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
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_ba_detail",
                queryPath: @"Queries\pengaduan_non_pelanggan\ba_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_non_pelanggan_ba_detail",
                queryPath: @"Queries\pengaduan_non_pelanggan\ba_detail2.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                });

            await Utils.LoketConnectionWrapper(async (conn, trans) =>
            {
                await conn.ExecuteAsync(@"DROP TABLE IF EXISTS __tmp_tipepermohonan", transaction: trans);
            });
        }
    }
}
