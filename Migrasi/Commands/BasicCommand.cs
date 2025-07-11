using Dapper;
using Migrasi.Helpers;
using Spectre.Console;
using Spectre.Console.Cli;
using Sprache;

namespace Migrasi.Commands
{
    public class BasicCommand : AsyncCommand<BasicCommand.Settings>
    {
        public class Settings : CommandSettings
        {
            [CommandArgument(0, "<idpdam>")]
            public int? IdPdam { get; set; }
        }

        public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
        {
            //TODO: hanya proses data master yg blm ada di basic
            const string MASTER_DATA = "MASTER_DATA";
            const string BAYAR_AIR = "BAYAR_AIR";
            const string PIUTANG_NONAIR = "PIUTANG_NONAIR";
            const string BAYAR_NONAIR = "BAYAR_NONAIR";
            const string ANGSURAN_NONAIR = "ANGSURAN_NONAIR";
            const string PERMOHONAN_SAMBUNG_BARU = "PERMOHONAN_SAMBUNG_BARU";
            const string PERMOHONAN_BALIK_NAMA = "PERMOHONAN_BALIK_NAMA";
            const string PERMOHONAN_BUKA_SEGEL = "PERMOHONAN_BUKA_SEGEL";
            const string PERMOHONAN_KOREKSI_DATA = "PERMOHONAN_KOREKSI_DATA";
            const string PERMOHONAN_KOREKSI_REKENING = "PERMOHONAN_KOREKSI_REKENING";
            const string PERMOHONAN_TUTUP_TOTAL = "PERMOHONAN_TUTUP_TOTAL";
            const string PERMOHONAN_RUBAH_GOL = "PERMOHONAN_RUBAH_GOL";
            const string PERMOHONAN_RUBAH_RAYON = "PERMOHONAN_RUBAH_RAYON";
            const string PERMOHONAN_SAMBUNG_KEMBALI = "PERMOHONAN_SAMBUNG_KEMBALI";
            const string PERMOHONAN_SEGEL = "PERMOHONAN_SEGEL";
            const string PENGADUAN_PLG = "PENGADUAN_PLG";
            const string PENGADUAN_NON_PLG = "PENGADUAN_NON_PLG";

            List<string> prosesList =
            [
                MASTER_DATA,
                BAYAR_AIR,
                PIUTANG_NONAIR,
                BAYAR_NONAIR,
                ANGSURAN_NONAIR,
                PERMOHONAN_SAMBUNG_BARU,
                PERMOHONAN_BALIK_NAMA,
                PERMOHONAN_BUKA_SEGEL,
                PERMOHONAN_KOREKSI_DATA,
                PERMOHONAN_KOREKSI_REKENING,
                PERMOHONAN_TUTUP_TOTAL,
                PERMOHONAN_RUBAH_GOL,
                PERMOHONAN_RUBAH_RAYON,
                PERMOHONAN_SAMBUNG_KEMBALI,
                PERMOHONAN_SEGEL,
                PENGADUAN_PLG,
                PENGADUAN_NON_PLG,
            ];

            string? namaPdam = "";
            await Utils.MainConnectionWrapper(async (conn, trans) =>
            {
                namaPdam = await conn.QueryFirstOrDefaultAsync<string>(@"SELECT namapdam FROM master_attribute_pdam WHERE idpdam=@idpdam", new { idpdam = settings.IdPdam }, trans);
            });
            AnsiConsole.WriteLine($"{settings.IdPdam} {namaPdam}");

            var selectedProses = AnsiConsole.Prompt(
                new MultiSelectionPrompt<string>()
                .Title("Pilih proses:")
                .Required()
                .InstructionsText("[grey](Press [blue]<space>[/] to toggle, [green]<enter>[/] to accept)[/]")
                .MoreChoicesText("[grey](Move up and down to reveal more options)[/]")
                .AddChoices(prosesList));

            var prosesMaster = selectedProses.Exists(s => s == MASTER_DATA);
            var prosesBayarAir = selectedProses.Exists(s => s == BAYAR_AIR);
            var prosesPiutangNonair = selectedProses.Exists(s => s == PIUTANG_NONAIR);
            var prosesBayarNonair = selectedProses.Exists(s => s == BAYAR_NONAIR);
            var prosesAngsuranNonair = selectedProses.Exists(s => s == ANGSURAN_NONAIR);
            var prosesPermohonanSambungBaru = selectedProses.Exists(s => s == PERMOHONAN_SAMBUNG_BARU);
            var prosesPermohonanBalikNama = selectedProses.Exists(s => s == PERMOHONAN_BALIK_NAMA);
            var prosesPermohonanBukaSegel = selectedProses.Exists(s => s == PERMOHONAN_BUKA_SEGEL);
            var prosesPermohonanKoreksiData = selectedProses.Exists(s => s == PERMOHONAN_KOREKSI_DATA);
            var prosesPermohonanKoreksiRekening = selectedProses.Exists(s => s == PERMOHONAN_KOREKSI_REKENING);
            var prosesPermohonanTutupTotal = selectedProses.Exists(s => s == PERMOHONAN_TUTUP_TOTAL);
            var prosesPermohonanRubahGol = selectedProses.Exists(s => s == PERMOHONAN_RUBAH_GOL);
            var prosesPermohonanRubahRayon = selectedProses.Exists(s => s == PERMOHONAN_RUBAH_RAYON);
            var prosesPermohonanSambungKembali = selectedProses.Exists(s => s == PERMOHONAN_SAMBUNG_KEMBALI);
            var prosesPermohonanSegel = selectedProses.Exists(s => s == PERMOHONAN_SEGEL);
            var prosesPengaduanPlg = selectedProses.Exists(s => s == PENGADUAN_PLG);
            var prosesPengaduanNonPlg = selectedProses.Exists(s => s == PENGADUAN_NON_PLG);

            AnsiConsole.WriteLine("Proses dipilih:");
            AnsiConsole.Write(new Rows(selectedProses.Select(s => new Text($"- {s}")).ToList()));

            if (!Utils.ConfirmationPrompt("Yakin untuk melanjutkan?"))
            {
                return 0;
            }

            try
            {
                await AnsiConsole.Status()
                    .StartAsync("Sedang diproses...", async _ =>
                    {
                        if (prosesMaster)
                        {
                            await MasterData(settings);
                            await PaketMaterial(settings);
                            await PaketOngkos(settings);
                            await PaketRab(settings);
                        }

                        if (prosesBayarAir)
                        {
                            await BayarAir(settings);
                        }

                        if (prosesPiutangNonair)
                        {
                            await PiutangNonair(settings);
                        }

                        if (prosesBayarNonair)
                        {
                            await BayarNonair(settings);
                        }

                        if (prosesAngsuranNonair)
                        {
                            await AngsuranNonair(settings);
                        }

                        if (prosesPermohonanSambungBaru)
                        {
                            await SambungBaru(settings);
                        }

                        if (prosesPermohonanBalikNama)
                        {
                            await BalikNama(settings);
                        }

                        if (prosesPermohonanBukaSegel)
                        {
                            await BukaSegel(settings);
                        }

                        if (prosesPermohonanKoreksiData)
                        {
                            await KoreksiData(settings);
                        }

                        if (prosesPermohonanKoreksiRekening)
                        {
                            await KoreksiRekair(settings);
                        }

                        if (prosesPermohonanTutupTotal)
                        {
                            await TutupTotal(settings);
                        }

                        if (prosesPermohonanRubahGol)
                        {
                            await RubahGolongan(settings);
                        }

                        if (prosesPermohonanRubahRayon)
                        {
                            await RubahRayon(settings);
                        }

                        if (prosesPermohonanSambungKembali)
                        {
                            await SambungKembali(settings);
                        }

                        if (prosesPermohonanSegel)
                        {
                            await Segel(settings);
                        }

                        if (prosesPengaduanPlg)
                        {
                            await PengaduanPlg(settings);
                        }

                        if (prosesPengaduanNonPlg)
                        {
                            await PengaduanNonPlg(settings);
                        }
                    });
            }
            catch (Exception)
            {
                throw;
            }

            return 0;
        }

        private static async Task PengaduanNonPlg(Settings settings)
        {
            await Utils.TrackProgress("pengaduannonplg|permohonan_non_pelanggan", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_non_pelanggan",
                    queryPath: @"queries\pengaduan_non_pelanggan\pengaduan.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("pengaduannonplg|permohonan_non_pelanggan_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_non_pelanggan_detail",
                    queryPath: @"queries\pengaduan_non_pelanggan\detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("pengaduannonplg|permohonan_non_pelanggan_spk_pasang", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_non_pelanggan_spk_pasang",
                    queryPath: @"queries\pengaduan_non_pelanggan\spk_pasang.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("pengaduannonplg|permohonan_non_pelanggan_ba", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_non_pelanggan_ba",
                    queryPath: @"queries\pengaduan_non_pelanggan\ba.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("pengaduannonplg|permohonan_non_pelanggan_ba_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_non_pelanggan_ba_detail",
                    queryPath: @"queries\pengaduan_non_pelanggan\ba_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });
        }

        private static async Task PengaduanPlg(Settings settings)
        {
            await Utils.TrackProgress("pengaduanplg|permohonan_pelanggan_air", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air",
                    queryPath: @"queries\pengaduan_pelanggan\pengaduan.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("pengaduanplg|permohonan_pelanggan_air_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_detail",
                    queryPath: @"queries\pengaduan_pelanggan\detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("pengaduanplg|permohonan_pelanggan_air_spk_pasang", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_spk_pasang",
                    queryPath: @"queries\pengaduan_pelanggan\spk_pasang.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("pengaduanplg|permohonan_pelanggan_air_ba", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_ba",
                    queryPath: @"queries\pengaduan_pelanggan\ba.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("pengaduanplg|permohonan_pelanggan_air_ba_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_ba_detail",
                    queryPath: @"queries\pengaduan_pelanggan\ba_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });
        }

        private static async Task Segel(Settings settings)
        {
            await Utils.TrackProgress("segel|permohonan_pelanggan_air", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air",
                    queryPath: @"queries\segel\segel.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("segel|permohonan_pelanggan_air_spk_pasang", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_spk_pasang",
                    queryPath: @"queries\segel\spk_pasang.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("segel|permohonan_pelanggan_air_ba", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_ba",
                    queryPath: @"queries\segel\ba.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("segel|permohonan_pelanggan_air_ba_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_ba_detail",
                    queryPath: @"queries\segel\ba_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });
        }

        private static async Task LoadDataMaster(Settings settings)
        {
            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_blok",
                query: @"select * from master_attribute_blok where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_jenis_bangunan",
                query: @"select * from master_attribute_jenis_bangunan where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_jenis_nonair",
                query: @"select * from master_attribute_jenis_nonair where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_kelainan",
                query: @"select * from master_attribute_kelainan where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_kelurahan",
                query: @"select * from master_attribute_kelurahan where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_kepemilikan",
                query: @"select * from master_attribute_kepemilikan where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_kolektif",
                query: @"select * from master_attribute_kolektif where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_kondisi_meter",
                query: @"select * from master_attribute_kondisi_meter where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_kwh",
                query: @"select * from master_attribute_kwh where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_loket",
                query: @"select * from master_attribute_loket where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_material",
                query: @"select * from master_attribute_material where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_material_paket",
                query: @"select * from master_attribute_material_paket where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_material_paket_detail",
                query: @"select * from master_attribute_material_paket_detail where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_merek_meter",
                query: @"select * from master_attribute_merek_meter where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_ongkos",
                query: @"select * from master_attribute_ongkos where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_ongkos_paket",
                query: @"select * from master_attribute_ongkos_paket where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_ongkos_paket_detail",
                query: @"select * from master_attribute_ongkos_paket_detail where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_paket",
                query: @"select * from master_attribute_paket where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_pekerjaan",
                query: @"select * from master_attribute_pekerjaan where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
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
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_petugas_baca",
                query: @"select * from master_attribute_petugas_baca where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_rayon",
                query: @"select * from master_attribute_rayon where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_sumber_air",
                query: @"select * from master_attribute_sumber_air where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_attribute_tipe_pendaftaran_sambungan",
                query: @"select * from master_attribute_tipe_pendaftaran_sambungan where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
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
                targetConnection: AppSettings.TampungConnectionString,
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
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_periode",
                query: @"select * from master_periode where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_tarif_diameter",
                query: @"select * from master_tarif_diameter where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_tarif_golongan",
                query: @"select * from master_tarif_golongan where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "master_user",
                query: @"select * from master_user where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "personalia_master_attribute_urusan_pegawai",
                query: @"select * from personalia_master_attribute_urusan_pegawai where idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "tampung_master_pelanggan_air",
                query: @"SELECT idpdam,`idpelangganair`,`nosamb` FROM `master_pelanggan_air` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "tampung_rekening_nonair",
                query: @"SELECT idpdam,`idnonair`,`nomornonair`,urutan FROM `rekening_nonair` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "tampung_permohonan_non_pelanggan",
                query: @"SELECT idpdam,`idpermohonan`,idtipepermohonan,`nomorpermohonan` FROM `permohonan_non_pelanggan` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "tampung_permohonan_pelanggan_air",
                query: @"SELECT idpdam,`idpermohonan`,idtipepermohonan,`nomorpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
                table: "tampung_koreksi_data",
                query: @"SELECT idpdam,`idkoreksi`,nomor FROM `master_pelanggan_air_riwayat_koreksi` WHERE idpdam=@idpdam",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

        }

        private static async Task PiutangNonair(Settings settings)
        {
            await Utils.TrackProgress("piutang|rekening_nonair", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_nonair",
                    queryPath: @"queries\nonair\nonair.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("piutang|rekening_nonair_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_nonair_detail",
                    queryPath: @"queries\nonair\nonair_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });
        }

        private static async Task BayarNonair(Settings settings)
        {
            await Utils.TrackProgress("bayar|rekening_nonair_transaksi", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_nonair_transaksi",
                    queryPath: @"queries\nonair\nonair_transaksi.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });
        }

        private static async Task BayarAir(Settings settings)
        {
            await Utils.TrackProgress($"bayar|rekening_air_transaksi", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_air_transaksi",
                    queryPath: @"queries\bayar\bayar_transaksi.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });
        }

        private static async Task AngsuranNonair(Settings settings)
        {
            await Utils.TrackProgress("angsurannonair|rekening_nonair_angsuran", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_nonair_angsuran",
                    queryPath: @"queries\angsuran_nonair\nonair_angsuran.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("angsurannonair|rekening_nonair_angsuran_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "rekening_nonair_angsuran_detail",
                    queryPath: @"queries\angsuran_nonair\nonair_angsuran_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });
        }

        private static async Task KoreksiData(Settings settings)
        {
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_pelanggan_air_riwayat_koreksi",
                queryPath: @"queries\koreksi_data\koreksi_data.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                });

            //copy terbaru
            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
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
                queryPath: @"queries\koreksi_data\koreksi_data_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastIdKoreksiDetail },
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
                    queryPath: @"queries\master\master_attribute_paket.sql",
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
                    queryPath: @"queries\master\paket_ongkos\master_attribute_ongkos.sql",
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
                    queryPath: @"queries\master\paket_ongkos\master_attribute_ongkos_paket.sql",
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
                    queryPath: @"queries\master\paket_ongkos\master_attribute_ongkos_paket_detail.sql",
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
                    queryPath: @"queries\master\paket_material\master_attribute_material.sql",
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
                    queryPath: @"queries\master\paket_material\master_attribute_material_paket.sql",
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
                    queryPath: @"queries\master\paket_material\master_attribute_material_paket_detail.sql",
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
                queryPath: @"queries\master\master_attribute_flag.sql",
                table: "master_attribute_flag",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_status",
                queryPath: @"queries\master\master_attribute_status.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_jenis_bangunan",
                queryPath: @"queries\master\master_attribute_jenis_bangunan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_kepemilikan",
                queryPath: @"queries\master\master_attribute_kepemilikan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_pekerjaan",
                queryPath: @"queries\master\master_attribute_pekerjaan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_peruntukan",
                queryPath: @"queries\master\master_attribute_peruntukan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_jenis_pipa",
                queryPath: @"queries\master\master_attribute_jenis_pipa.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_kwh",
                queryPath: @"queries\master\master_attribute_kwh.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_tarif_golongan",
                queryPath: @"queries\master\master_tarif_golongan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_tarif_golongan_detail",
                queryPath: @"queries\master\master_tarif_golongan_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_tarif_diameter",
                queryPath: @"queries\master\master_tarif_diameter.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_tarif_diameter_detail",
                queryPath: @"queries\master\master_tarif_diameter_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_wilayah",
                queryPath: @"queries\master\master_attribute_wilayah.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_area",
                queryPath: @"queries\master\master_attribute_area.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_rayon",
                queryPath: @"queries\master\master_attribute_rayon.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_blok",
                queryPath: @"queries\master\master_attribute_blok.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_cabang",
                queryPath: @"queries\master\master_attribute_cabang.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_kecamatan",
                queryPath: @"queries\master\master_attribute_kecamatan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_kelurahan",
                queryPath: @"queries\master\master_attribute_kelurahan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_dma",
                queryPath: @"queries\master\master_attribute_dma.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_dmz",
                queryPath: @"queries\master\master_attribute_dmz.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_tarif_administrasi_lain",
                queryPath: @"queries\master\master_tarif_administrasi_lain.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_tarif_pemeliharaan_lain",
                queryPath: @"queries\master\master_tarif_pemeliharaan_lain.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_tarif_retribusi_lain",
                queryPath: @"queries\master\master_tarif_retribusi_lain.sql",
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
                queryPath: @"queries\master\master_attribute_kolektif.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_sumber_air",
                queryPath: @"queries\master\master_attribute_sumber_air.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_merek_meter",
                queryPath: @"queries\master\master_attribute_merek_meter.sql",
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
                queryPath: @"queries\master\master_attribute_kondisi_meter.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_kelainan",
                queryPath: @"queries\master\master_attribute_kelainan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_petugas_baca",
                queryPath: @"queries\master\master_attribute_petugas_baca.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_periode",
                queryPath: @"queries\master\master_periode.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_periode_billing",
                queryPath: @"queries\master\master_periode_billing.sql",
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
                queryPath: @"queries\master\master_attribute_loket.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_user",
                queryPath: @"queries\master\master_user.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_tipe_pendaftaran_sambungan",
                queryPath: @"queries\master\master_attribute_tipe_pendaftaran_sambungan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "master_attribute_sumber_pengaduan",
                queryPath: @"queries\master\master_attribute_sumber_pengaduan.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "personalia_master_pegawai",
                queryPath: @"queries\master\personalia_master_pegawai.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });
        }

        private static async Task KoreksiRekair(Settings settings)
        {
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air",
                queryPath: @"queries\koreksi_rekair\koreksi_rekair.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                });

            //copy terbaru
            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
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
                queryPath: @"queries\koreksi_rekair\koreksi_rekair_periode.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                    { "@lastid", lastId },
                });
        }

        private static async Task SambungBaru(Settings settings)
        {
            await Utils.TrackProgress("sambbaru|permohonan_non_pelanggan", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_non_pelanggan",
                    queryPath: @"queries\sambung_baru\sambung_baru.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("sambbaru|permohonan_non_pelanggan_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_non_pelanggan_detail",
                    queryPath: @"queries\sambung_baru\detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("sambbaru|permohonan_non_pelanggan_spk", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_non_pelanggan_spk",
                    queryPath: @"queries\sambung_baru\spk.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("sambbaru|permohonan_non_pelanggan_spk_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_non_pelanggan_spk_detail",
                    queryPath: @"queries\sambung_baru\spk_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("sambbaru|permohonan_non_pelanggan_rab", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_non_pelanggan_rab",
                    queryPath: @"queries\sambung_baru\rab.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("sambbaru|permohonan_non_pelanggan_rab_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_non_pelanggan_rab_detail",
                    queryPath: @"queries\sambung_baru\rab_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("sambbaru|permohonan_non_pelanggan_spk_pasang", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_non_pelanggan_spk_pasang",
                    queryPath: @"queries\sambung_baru\spk_pasang.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("sambbaru|permohonan_non_pelanggan_ba", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_non_pelanggan_ba",
                    queryPath: @"queries\sambung_baru\ba.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("sambbaru|permohonan_non_pelanggan_ba_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_non_pelanggan_ba_detail",
                    queryPath: @"queries\sambung_baru\ba_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });
        }

        private static async Task BukaSegel(Settings settings)
        {
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air",
                queryPath: @"queries\buka_segel\buka_segel.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            //copy terbaru
            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
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
                queryPath: @"queries\buka_segel\detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_spk_pasang",
                queryPath: @"queries\buka_segel\spk_pasang.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba",
                queryPath: @"queries\buka_segel\ba.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba_detail",
                queryPath: @"queries\buka_segel\ba_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam }
                });
        }

        private static async Task SambungKembali(Settings settings)
        {
            await Utils.TrackProgress("sambkembali|permohonan_pelanggan_air", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air",
                    queryPath: @"queries\sambung_kembali\sambung_kembali.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("sambkembali|permohonan_pelanggan_air_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_detail",
                    queryPath: @"queries\sambung_kembali\detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("sambkembali|permohonan_pelanggan_air_spk", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_spk",
                    queryPath: @"queries\sambung_kembali\spk.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("sambkembali|permohonan_pelanggan_air_spk_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_spk_detail",
                    queryPath: @"queries\sambung_kembali\spk_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("sambkembali|permohonan_pelanggan_air_rab", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_rab",
                    queryPath: @"queries\sambung_kembali\rab.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("sambkembali|permohonan_pelanggan_air_rab_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_rab_detail",
                    queryPath: @"queries\sambung_kembali\rab_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("sambkembali|permohonan_pelanggan_air_spk_pasang", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_spk_pasang",
                    queryPath: @"queries\sambung_kembali\spk_pasang.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            //await Utils.TrackProgress("sambkembali|permohonan_pelanggan_air_ba", async () =>
            //{
            //    await Utils.BulkCopy(
            //        sourceConnection: AppSettings.LoketConnectionString,
            //        targetConnection: AppSettings.MainConnectionString,
            //        table: "permohonan_pelanggan_air_ba",
            //        queryPath: @"queries\sambung_kembali\ba.sql",
            //        parameters: new()
            //        {
            //            { "@idpdam", settings.IdPdam },
            //        });
            //});

            //await Utils.TrackProgress("sambkembali|permohonan_pelanggan_air_ba_detail", async () =>
            //{
            //    await Utils.BulkCopy(
            //        sourceConnection: AppSettings.LoketConnectionString,
            //        targetConnection: AppSettings.MainConnectionString,
            //        table: "permohonan_pelanggan_air_ba_detail",
            //        queryPath: @"queries\sambung_kembali\ba_detail.sql",
            //        parameters: new()
            //        {
            //            { "@idpdam", settings.IdPdam },
            //        });
            //});
        }

        private static async Task RubahRayon(Settings settings)
        {
            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air",
                queryPath: @"queries\rubah_rayon\rubah_rayon.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                });

            //copy terbaru
            await Utils.CopyToDiffrentHost(
                sourceConnection: AppSettings.MainConnectionString,
                targetConnection: AppSettings.TampungConnectionString,
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
                queryPath: @"queries\rubah_rayon\detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba",
                queryPath: @"queries\rubah_rayon\ba.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                });

            await Utils.BulkCopy(
                sourceConnection: AppSettings.LoketConnectionString,
                targetConnection: AppSettings.MainConnectionString,
                table: "permohonan_pelanggan_air_ba_detail",
                queryPath: @"queries\rubah_rayon\ba_detail.sql",
                parameters: new()
                {
                    { "@idpdam", settings.IdPdam },
                });
        }

        private static async Task RubahGolongan(Settings settings)
        {
            await Utils.TrackProgress("rubahgol|permohonan_pelanggan_air", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air",
                    queryPath: @"queries\rubah_gol\rubah_gol.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("rubahgol|permohonan_pelanggan_air_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_detail",
                    queryPath: @"queries\rubah_gol\detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("rubahgol|permohonan_pelanggan_air_spk", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_spk",
                    queryPath: @"queries\rubah_gol\spk.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("rubahgol|permohonan_pelanggan_air_ba", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_ba",
                    queryPath: @"queries\rubah_gol\ba.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });
        }

        private static async Task BalikNama(Settings settings)
        {
            await Utils.TrackProgress("baliknama|permohonan_pelanggan_air", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air",
                    queryPath: @"queries\balik_nama\balik_nama.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("baliknama|permohonan_pelanggan_air_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_detail",
                    queryPath: @"queries\balik_nama\detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });

            await Utils.TrackProgress("baliknama|permohonan_pelanggan_air_ba", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_ba",
                    queryPath: @"queries\balik_nama\ba.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam }
                    });
            });
        }

        private static async Task TutupTotal(Settings settings)
        {
            await Utils.TrackProgress("tutuptotal|permohonan_pelanggan_air", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air",
                    queryPath: @"queries\tutup_total\tutup_total.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("tutuptotal|permohonan_pelanggan_air_spk_pasang", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_spk_pasang",
                    queryPath: @"queries\tutup_total\spk_pasang.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("tutuptotal|permohonan_pelanggan_air_ba", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_ba",
                    queryPath: @"queries\tutup_total\ba.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });

            await Utils.TrackProgress("tutuptotal|permohonan_pelanggan_air_ba_detail", async () =>
            {
                await Utils.BulkCopy(
                    sourceConnection: AppSettings.LoketConnectionString,
                    targetConnection: AppSettings.MainConnectionString,
                    table: "permohonan_pelanggan_air_ba_detail",
                    queryPath: @"queries\tutup_total\ba_detail.sql",
                    parameters: new()
                    {
                        { "@idpdam", settings.IdPdam },
                    });
            });
        }
    }
}
