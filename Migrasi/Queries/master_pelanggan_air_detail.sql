SELECT
 @idpdam,
 @id := @id+ 1 AS idpelangganair,
 IFNULL(sua.id, -1) AS idsumberair,
 -1 AS iddma,
 -1 AS iddmz,
 IFNULL(blo.id, -1) AS idblok,
 IFNULL(mer.id, -1) AS idmerekmeter,
 IFNULL(kon.id, 1) AS idkondisimeter,
 IFNULL(adm.id, -1) AS idadministrasilain,
 IFNULL(pem.id, -1) AS idpemeliharaanlain,
 IFNULL(ret.id, -1) AS idretribusilain,
 -1 AS idpekerjaan,
 -1 AS idjenisbangunan,
 -1 AS idperuntukan,
 -1 AS idkepemilikan,
 pel.nosegelmeter AS nosegel,
 pel.nohp,
 pel.notelp,
 IF(pel.noktp='' OR pel.noktp IS NULL, '000', pel.noktp) AS noktp,
 '000' AS nokk,
 pel.email,
 IF(pel.serimeter='' OR pel.serimeter IS NULL, '000', pel.serimeter) AS noserimeter,
 pel.tglmeter,
 '' AS pekerjaan,
 IF(pel.penghuni='' OR pel.penghuni IS NULL, 0, pel.penghuni) AS penghuni,
 IF(pel.namapemilik='' OR pel.namapemilik IS NULL, pel.nama, pel.namapemilik) AS namapemilik,
 pel.alamat AS alamatpemilik,
 '000' AS kodepost,
 0 AS dayalistrik,
 0 AS luastanah,
 IF(pel.luasrumah='' OR pel.luasrumah IS NULL, 0, pel.luasrumah) AS luasrumah,
 IF(pel.urutanbaca='' OR pel.urutanbaca IS NULL, 0, pel.urutanbaca) AS urutanbaca,
 IF(pel.stan_awal_pasang='' OR pel.stan_awal_pasang IS NULL, 0, pel.stan_awal_pasang) AS stanawalpasang,
 LEFT(pel.nopendaftaran, 30) AS nopendaftaran,
 pel.tgldaftar,
 NULL AS tglpenentuanbaca,
 LEFT(pel.norab, 30) AS norab,
 '' AS nobapemasangan,
 pel.tgldaftar AS tglpasang,
 pel.keterangan,
 NOW() AS waktuupdate
FROM
 pelanggan pel
 LEFT JOIN sumberair sua ON sua.kodesumberair = pel.kodesumberair
 LEFT JOIN blok blo ON blo.kodeblok = pel.kodeblok
 LEFT JOIN merkmeter mer ON mer.merk = pel.merkmeter
 LEFT JOIN (
   	SELECT 
   	@idKondisiMeter := @idKondisiMeter + 1 AS id,
   	kodekondisi,
   	kondisi
   	FROM kondisimeter
   	,(SELECT @idKondisiMeter := 0) AS idKondisiMeter
 ) kon ON kon.kodekondisi = pel.kodekondisimeter
 LEFT JOIN byadministrasi_lain adm ON adm.kode = pel.kodeadministrasilain
 LEFT JOIN bypemeliharaan_lain pem ON pem.kode = pel.kodepemeliharaanlain
 LEFT JOIN byretribusi_lain ret ON ret.kode = pel.koderetribusilain
 ,(SELECT @id := @lastId) AS id;