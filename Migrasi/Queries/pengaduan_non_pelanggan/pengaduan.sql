DROP TEMPORARY TABLE IF EXISTS __tmp_sumberpengaduan;
CREATE TEMPORARY TABLE __tmp_sumberpengaduan AS
SELECT
@id := @id+1 AS id,
sumberpengaduan
FROM sumberpengaduan
,(SELECT @id := 0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_userloket;
CREATE TEMPORARY TABLE __tmp_userloket AS
SELECT
@idpdam,
@id := @id + 1 AS iduser,
a.nama,
a.namauser
FROM (
SELECT nama,namauser,`passworduser`,alamat,aktif FROM [bacameter].`userakses`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,aktif FROM [bsbs].`userakses`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM `userloket`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM `userbshl`
) a,
(SELECT @id := 0) AS id
GROUP BY a.namauser;

DROP TEMPORARY TABLE IF EXISTS __tmp_golongan;
CREATE TEMPORARY TABLE __tmp_golongan AS
SELECT
@id:=@id+1 AS id,
kodegol,
aktif
FROM
golongan,
(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_pengaduan;
CREATE TEMPORARY TABLE __tmp_pengaduan AS
SELECT
@id:=@id+1 AS id,
p.nomor,
p.`kategori`
FROM pengaduan p
JOIN `__tmp_tipepermohonan` t ON t.`kodejenisnonair`=p.`kategori`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0 AND `flagpel`=0;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
t.idtipepermohonan AS `idtipepermohonan`,
s.id AS `idsumberpengaduan`,
pp.`nomor` AS `nomorpermohonan`,
pp.`tglditerima` AS `waktupermohonan`,
0 AS `flagpendaftaran`,
NULL AS `idtipependaftaransambungan`,
pp.`namapelapor` AS `nama`,
LEFT(pp.`alamatpelapor`,200) AS `alamat`,
NULL AS `idgolongan`,
0 AS `iddiameter`,
r.`id` AS `idrayon`,
k.`id` AS `idkelurahan`,
NULL AS `idblok`,
NULL AS `idperuntukan`,
NULL AS `idjenisbangunan`,
NULL AS `idkepemilikan`,
NULL AS `idpekerjaan`,
NULL AS `idkolektif`,
NULL AS `idsumberair`,
NULL AS `iddma`,
NULL AS `iddmz`,
NULL AS `idmerekmeter`,
NULL AS `idkondisimeter`,
NULL AS `idadministrasilain`,
NULL AS `idpemeliharaanlain`,
NULL AS `idretribusilain`,
NULL AS `noserimeter`,
NULL AS `tglmeter`,
NULL AS `urutanbaca`,
NULL AS `stanawalpasang`,
LEFT(pp.`notelppelapor`,20) AS `notelp`,
NULL AS `email`,
NULL AS `noktp`,
NULL AS `nokk`,
NULL AS `kodepost`,
0 AS `dayalistrik`,
0 AS `luastanah`,
0 AS `luasrumah`,
NULL AS `rt`,
NULL AS `rw`,
LEFT(pp.`nohppelapor`,20) AS `nohp`,
pp.`uraianlaporan` AS `keterangan`,
NULL AS `nosambyangdiberikan`,
NULL AS `nosambdepan`,
NULL AS `nosambbelakang`,
NULL AS `nosambkiri`,
NULL AS `nosambkanan`,
0 AS `penghuni`,
NULL AS `namapemilik`,
NULL AS `alamatpemilik`,
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
0 AS `flagverifikasi`,
NULL AS `waktuverifikasi`,
0 AS `flagusulan`,
0 AS `flaglanjutkelanggananlimbah`,
0 AS `flagpelanggankavlingan`,
0 AS `flaghapus`,
COALESCE(pp.`tglditerima`,NOW()) AS `waktuupdate`,
NULL AS `airyangdigunakansebelumnya`,
IF(ba.nomorba IS NOT NULL,
 'Selesai',
 IF(ba.nomorspk IS NOT NULL,
  'Menunggu Berita Acara',
  'Menunggu SPK Pemasangan')) AS statuspermohonan
FROM
__tmp_pengaduan p
JOIN pengaduan pp ON p.nomor=pp.nomor
LEFT JOIN spk_pengaduan ba ON ba.nomorpengaduan=pp.nomor
LEFT JOIN __tmp_userloket usr ON usr.nama=pp.user
LEFT JOIN __tmp_sumberpengaduan s ON s.sumberpengaduan=pp.sumberpengaduan
LEFT JOIN __tmp_tipepermohonan t ON t.kodejenisnonair=pp.kategori
LEFT JOIN [bsbs].rayon r ON r.koderayon=pp.koderayon
LEFT JOIN [bsbs].kelurahan k ON k.kodekelurahan=pp.kodekelurahan
LEFT JOIN __tmp_golongan g ON g.kodegol=pp.kodegol AND g.aktif=1