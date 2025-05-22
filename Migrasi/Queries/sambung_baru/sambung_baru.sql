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
NULL AS `idkategorikawasan`,
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
NULL AS `fotoktp`,
NULL AS `fotokk`,
NULL AS `fotosuratpernyataan`,
NULL AS `fotoimb`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
ra.flagaktif AS `flagverifikasi`,
ra.waktupengaktifan AS `waktuverifikasi`,
0 AS `flagusulan`,
0 AS `flaglanjutkelanggananlimbah`,
p.`pelanggan_kavling` AS `flagpelanggankavlingan`,
p.`flaghapus` AS `flaghapus`,
p.`tgldaftar` AS `waktuupdate`,
NULL AS `airyangdigunakansebelumnya`,
NULL AS `statuspermohonan`,
0 AS `flagworkorder`
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