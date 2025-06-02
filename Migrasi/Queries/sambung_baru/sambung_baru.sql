-- permohonan_non_pelanggan
-- new(0, "idpdam")
-- new(1, "idpermohonan")
-- new(2, "idtipepermohonan")
-- new(3, "idsumberpengaduan")
-- new(4, "nomorpermohonan")
-- new(5, "waktupermohonan")
-- new(6, "flagpendaftaran")
-- new(7, "idtipependaftaransambungan")
-- new(8, "nama")
-- new(9, "alamat")
-- new(10, "idgolongan")
-- new(11, "iddiameter")
-- new(12, "idrayon")
-- new(13, "idkelurahan")
-- new(14, "idblok")
-- new(15, "idperuntukan")
-- new(16, "idjenisbangunan")
-- new(17, "idkepemilikan")
-- new(18, "idpekerjaan")
-- new(19, "idkolektif")
-- new(20, "idsumberair")
-- new(21, "iddma")
-- new(22, "iddmz")
-- new(23, "idmerekmeter")
-- new(24, "idkondisimeter")
-- new(25, "idadministrasilain")
-- new(26, "idpemeliharaanlain")
-- new(27, "idretribusilain")
-- new(28, "noserimeter")
-- new(29, "tglmeter")
-- new(30, "urutanbaca")
-- new(31, "stanawalpasang")
-- new(32, "notelp")
-- new(33, "email")
-- new(34, "noktp")
-- new(35, "nokk")
-- new(36, "kodepost")
-- new(37, "dayalistrik")
-- new(38, "luastanah")
-- new(39, "luasrumah")
-- new(40, "rt")
-- new(41, "rw")
-- new(42, "nohp")
-- new(43, "keterangan")
-- new(44, "nosambyangdiberikan")
-- new(45, "nosambdepan")
-- new(46, "nosambbelakang")
-- new(47, "nosambkiri")
-- new(48, "nosambkanan")
-- new(49, "penghuni")
-- new(50, "namapemilik")
-- new(51, "alamatpemilik")
-- new(52, "iduser")
-- new(53, "idnonair")
-- new(54, "latitude")
-- new(55, "longitude")
-- new(56, "alamatmap")
-- new(57, "flagverifikasi")
-- new(58, "waktuverifikasi")
-- new(59, "flagpelanggankavlingan")
-- new(60, "flaghapus")
-- new(61, "waktuupdate")
-- new(62, "airyangdigunakansebelumnya")
-- new(63, "statuspermohonan")

SET @maxid=(SELECT COALESCE(MAX(idpermohonan),0) AS maxid FROM [dataawal].`tampung_permohonan_non_pelanggan` WHERE idpdam=@idpdam);
SET @idtipepermohonan=(SELECT idtipepermohonan FROM [dataawal].`master_attribute_tipe_permohonan` WHERE idpdam=@idpdam AND `kodetipepermohonan`='SAMBUNGAN_BARU_AIR');

SELECT
@idpdam AS `idpdam`,
@id:=@id+1 AS `idpermohonan`,
@idtipepermohonan AS `idtipepermohonan`,
NULL AS `idsumberpengaduan`,
p.`nomorreg` AS `nomorpermohonan`,
p.`tgldaftar` AS `waktupermohonan`,
1 AS `flagpendaftaran`,
t.`idtipependaftaransambungan` AS `idtipependaftaransambungan`,
p.`nama` AS `nama`,
p.`alamat` AS `alamat`,
NULL AS `idgolongan`,
NULL AS `iddiameter`,
r.`idrayon` AS `idrayon`,
k.`idkelurahan` AS `idkelurahan`,
b.`idblok` AS `idblok`,
COALESCE(pr.`idperuntukan`,-1) AS `idperuntukan`,
COALESCE(j.`idjenisbangunan`,-1) AS `idjenisbangunan`,
COALESCE(kp.`idkepemilikan`,-1) AS `idkepemilikan`,
COALESCE(pk.`idpekerjaan`,-1) AS `idpekerjaan`,
-1 AS `idkolektif`,
COALESCE(s.`idsumberair`,-1) AS `idsumberair`,
-1 AS `iddma`,
-1 AS `iddmz`,
-1 AS `idmerekmeter`,
-1 AS `idkondisimeter`,
-1 AS `idadministrasilain`,
-1 AS `idpemeliharaanlain`,
-1 AS `idretribusilain`,
NULL AS `noserimeter`,
NULL AS `tglmeter`,
NULL AS `urutanbaca`,
NULL AS `stanawalpasang`,
p.`telp` AS `notelp`,
p.`email` AS `email`,
p.`no_ktp` AS `noktp`,
p.`no_kk` AS `nokk`,
NULL AS `kodepost`,
p.`daya_listrik` AS `dayalistrik`,
p.`luas_tanah` AS `luastanah`,
p.`luas_rumah` AS `luasrumah`,
p.`rt` AS `rt`,
p.`rw` AS `rw`,
p.`hp` AS `nohp`,
p.`keterangan` AS `keterangan`,
p.`nosambdarireg` AS `nosambyangdiberikan`,
NULL AS `nosambdepan`,
NULL AS `nosambbelakang`,
NULL AS `nosambkiri`,
NULL AS `nosambkanan`,
p.`penghuni` AS `penghuni`,
p.`nama_pemilik` AS `namapemilik`,
p.`alamat_pemilik` AS `alamatpemilik`,
NULL AS `iduser`,
NULL AS `idnonair`,
NULL AS `latitude`,
NULL AS `longitude`,
NULL AS `alamatmap`,
ra.flagaktif AS `flagverifikasi`,
ra.waktupengaktifan AS `waktuverifikasi`,
p.`pelanggan_kavling` AS `flagpelanggankavlingan`,
p.`flaghapus` AS `flaghapus`,
p.`tgldaftar` AS `waktuupdate`,
NULL AS `airyangdigunakansebelumnya`,
NULL AS `statuspermohonan`
FROM `pendaftaran` p
LEFT JOIN [dataawal].`master_attribute_tipe_pendaftaran_sambungan` t ON t.`namatipependaftaransambungan`=p.`tipe` AND t.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_rayon` r ON r.koderayon=p.koderayon AND r.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_kelurahan` k ON k.kodekelurahan=p.kodekelurahan AND k.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_blok` b ON b.`kodeblok`=p.`kodeblok` AND b.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_peruntukan` pr ON pr.`namaperuntukan`=p.`peruntukan` AND pr.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_jenis_bangunan` j ON j.`namajenisbangunan`=p.`jenisbangunan` AND j.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_kepemilikan` kp ON kp.`namakepemilikan`=p.`kepemilikan` AND kp.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_pekerjaan` pk ON pk.`namapekerjaan`=p.`pekerjaan` AND pk.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_sumber_air` s ON s.`namasumberair`=p.`airyangdigunakansaatini` AND s.`idpdam`=@idpdam
LEFT JOIN rab ra ON ra.nomorreg=p.nomorreg AND ra.flaghapus=0 AND ra.flagaktif=1
,(SELECT @id:=@maxid) AS id
WHERE p.flaghapus=0
AND p.nomorreg NOT IN (SELECT `nomorpermohonan` FROM [dataawal].`tampung_permohonan_non_pelanggan` WHERE idpdam=@idpdam)
AND DATE_FORMAT(p.tgldaftar,'%Y%m') BETWEEN 202502 AND 202504