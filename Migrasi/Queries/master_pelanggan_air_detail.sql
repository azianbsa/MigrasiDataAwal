-- master_pelanggan_air_detail
-- new(0, "idpdam")
-- new(1, "idpelangganair")
-- new(2, "idsumberair")
-- new(3, "iddma")
-- new(4, "iddmz")
-- new(5, "idblok")
-- new(6, "idmerekmeter")
-- new(7, "idkondisimeter")
-- new(8, "idadministrasilain")
-- new(9, "idpemeliharaanlain")
-- new(10, "idretribusilain")
-- new(11, "idpekerjaan")
-- new(12, "idjenisbangunan")
-- new(13, "idperuntukan")
-- new(14, "idkepemilikan")
-- new(15, "nosegel")
-- new(16, "nohp")
-- new(17, "notelp")
-- new(18, "noktp")
-- new(19, "nokk")
-- new(20, "email")
-- new(21, "noserimeter")
-- new(22, "tglmeter")
-- new(23, "pekerjaan")
-- new(24, "penghuni")
-- new(25, "namapemilik")
-- new(26, "alamatpemilik")
-- new(27, "kodepost")
-- new(28, "dayalistrik")
-- new(29, "luastanah")
-- new(30, "luasrumah")
-- new(31, "urutanbaca")
-- new(32, "stanawalpasang")
-- new(33, "nopendaftaran")
-- new(34, "tgldaftar")
-- new(35, "tglpenentuanbaca")
-- new(36, "norab")
-- new(37, "nobapemasangan")
-- new(38, "tglpasang")
-- new(39, "tglputus")
-- new(40, "noserimeterlama")
-- new(41, "kategoriputus")
-- new(42, "idtipependaftaransambungan")
-- new(43, "idkategorikawasan")
-- new(44, "keterangan")
-- new(45, "waktuupdate")

SELECT
@idpdam,
p.`id` AS idpelangganair,
COALESCE(s.`idsumberair`,-1) AS idsumberair,
-1 AS iddma,
-1 AS iddmz,
-1 AS idblok,
COALESCE(m.`idmerekmeter`,-1) AS idmerekmeter,
COALESCE(k.`idkondisimeter`,-1) AS idkondisimeter,
-1 AS idadministrasilain,
-1 AS idpemeliharaanlain,
-1 AS idretribusilain,
-1 AS idpekerjaan,
-1 AS idjenisbangunan,
-1 AS idperuntukan,
COALESCE(kb.`idkepemilikan`,-1) AS idkepemilikan,
p.nosegelmeter AS nosegel,
p.nohp AS nohp,
p.notelp AS notelp,
p.noktp AS noktp,
'' AS nokk,
p.email AS email,
TRIM(p.serimeter) AS noserimeter,
p.tglmeter AS tglmeter,
LEFT(TRIM(p.pekerjaan),30) AS pekerjaan,
IF(p.penghuni='' OR p.penghuni IS NULL, 0, p.penghuni) AS penghuni,
IF(p.namapemilik='' OR p.namapemilik IS NULL, p.nama, p.namapemilik) AS namapemilik,
p.alamat AS alamatpemilik,
'' AS kodepost,
p.dayalistrik AS dayalistrik,
0 AS luastanah,
IF(p.luasrumah='' OR p.luasrumah IS NULL, 0, p.luasrumah) AS luasrumah,
IF(p.urutanbaca='' OR p.urutanbaca IS NULL, 0, p.urutanbaca) AS urutanbaca,
IF(p.stan_awal_pasang='' OR p.stan_awal_pasang IS NULL, 0, p.stan_awal_pasang) AS stanawalpasang,
LEFT(p.nopendaftaran, 30) AS nopendaftaran,
p.tgldaftar AS tgldaftar,
NULL AS tglpenentuanbaca,
LEFT(p.norab, 30) AS norab,
'' AS nobapemasangan,
p.tgldaftar AS tglpasang,
p.tglputus AS tglputus,
NULL AS noserimeterlama,
CASE WHEN p.kategori_putus=4 THEN 'Permohonan' ELSE 'Tunggakan' END AS kategoriputus,
NULL AS idtipependaftaransambungan,
NULL AS idkategorikawasan,
p.keterangan AS keterangan,
p.waktuupdate AS waktuupdate
FROM pelanggan p
LEFT JOIN `kotaparepare_dataawal`.`master_attribute_sumber_air` s ON s.`kodesumberair`=p.kodesumberair AND s.idpdam=@idpdam
LEFT JOIN `kotaparepare_dataawal`.`master_attribute_merek_meter` m ON m.`namamerekmeter`=p.merkmeter AND m.`idpdam`=@idpdam
LEFT JOIN `kotaparepare_dataawal`.`master_attribute_kondisi_meter` k ON k.`kodekondisimeter`=p.kodekondisimeter AND k.`idpdam`=@idpdam
LEFT JOIN `kotaparepare_dataawal`.`master_attribute_kepemilikan` kb ON kb.`namakepemilikan`=p.kepemilikanbangunan AND kb.`idpdam`=@idpdam