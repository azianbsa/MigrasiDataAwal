DROP TABLE IF EXISTS __tmp_pendaftaran;
CREATE TABLE __tmp_pendaftaran AS
SELECT
@id:=@id+1 AS id,
nomorreg
FROM `pendaftaran`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

DROP TEMPORARY TABLE IF EXISTS __tmp_tipesambungan;
CREATE TEMPORARY TABLE __tmp_tipesambungan AS
SELECT @id:=@id+1 AS id,tipe 
FROM `tipesambungan`,(SELECT @id:=0) AS id;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
@tipepermohonan AS `idtipepermohonan`,
NULL AS `idsumberpengaduan`,
pp.`nomorreg` AS `nomorpermohonan`,
pp.`tgldaftar` AS `waktupermohonan`,
1 AS `flagpendaftaran`,
tp.id AS `idtipependaftaransambungan`,
pp.`nama` AS `nama`,
pp.`alamat` AS `alamat`,
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
pp.`telp` AS `notelp`,
pp.`email` AS `email`,
pp.`no_ktp` AS `noktp`,
pp.`no_kk` AS `nokk`,
NULL AS `kodepost`,
pp.`daya_listrik` AS `dayalistrik`,
pp.`luas_tanah` AS `luastanah`,
pp.`luas_rumah` AS `luasrumah`,
pp.`rt` AS `rt`,
pp.`rw` AS `rw`,
pp.`hp` AS `nohp`,
pp.`keterangan` AS `keterangan`,
pp.`nosambdarireg` AS `nosambyangdiberikan`,
NULL AS `nosambdepan`,
NULL AS `nosambbelakang`,
NULL AS `nosambkiri`,
NULL AS `nosambkanan`,
pp.`penghuni` AS `penghuni`,
pp.`nama_pemilik` AS `namapemilik`,
pp.`alamat_pemilik` AS `alamatpemilik`,
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
IF(v.`waktuverifikasi` IS NULL,0,1) AS `flagverifikasi`,
v.`waktuverifikasi` AS `waktuverifikasi`,
0 AS `flagusulan`,
0 AS `flaglanjutkelanggananlimbah`,
pp.`pelanggan_kavling` AS `flagpelanggankavlingan`,
0 AS `flaghapus`,
COALESCE(v.`waktuverifikasi`,rab.`tglpasang`,pp.`tgldaftar`,NOW()) AS `waktuupdate`,
NULL AS `airyangdigunakansebelumnya`,
IF(v.`waktuverifikasi` IS NULL,
 IF(rab.`tglpasang` IS NULL,
  IF(rab.`tanggalspkp` IS NULL,
   IF(rab.`norab` IS NULL,
    'Menunggu SPK Survey',
    'Menunggu RAB'),
   'Menunggu SPK Pemasangan'),
  'Menunggu Verifikasi'),
 'Selesai') AS `statuspermohonan`
FROM __tmp_pendaftaran p
JOIN `pendaftaran` pp ON pp.`nomorreg`=p.`nomorreg`
LEFT JOIN __tmp_tipesambungan tp ON tp.tipe=pp.`tipe`
LEFT JOIN [bsbs].rayon r ON r.koderayon=pp.koderayon
LEFT JOIN [bsbs].kelurahan k ON k.kodekelurahan=pp.kodekelurahan
LEFT JOIN `spk_opname_sambung_baru` spk ON spk.`nomorreg`=pp.`nomorreg` AND spk.`flaghapus`=0
LEFT JOIN `rab` rab ON rab.`nomorreg`=pp.`nomorreg` AND rab.`flaghapus`=0
LEFT JOIN `verifikasi` v ON v.`nomorba`=rab.`norab`