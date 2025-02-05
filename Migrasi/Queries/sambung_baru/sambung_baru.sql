﻿DROP TEMPORARY TABLE IF EXISTS __tmp_tipesambungan;
CREATE TEMPORARY TABLE __tmp_tipesambungan AS
SELECT @id:=@id+1 AS id,tipe 
FROM `tipesambungan`,(SELECT @id:=0) AS id;

SELECT
@idpdam AS `idpdam`,
@id := @id+1 AS `idpermohonan`,
@tipepermohonan AS `idtipepermohonan`,
NULL AS `idsumberpengaduan`,
p.`nomorreg` AS `nomorpermohonan`,
p.`tgldaftar` AS `waktupermohonan`,
1 AS `flagpendaftaran`,
tp.id AS `idtipependaftaransambungan`,
p.`nama` AS `nama`,
p.`alamat` AS `alamat`,
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
IF(rab.`tglpasang` IS NULL,0,1) AS `flagverifikasi`,
rab.`tglpasang` AS `waktuverifikasi`,
0 AS `flagusulan`,
0 AS `flaglanjutkelanggananlimbah`,
p.`pelanggan_kavling` AS `flagpelanggankavlingan`,
0 AS `flaghapus`,
COALESCE(rab.`tglpasang`,p.`tgldaftar`,NOW()) AS `waktuupdate`,
NULL AS `airyangdigunakansebelumnya`,
IF(rab.`tglpasang` IS NULL,
 IF(rab.`tanggalspkp` IS NULL,
  IF(rab.`tglrab` IS NULL,
   IF(spk.`tglspko` IS NULL,
    'Menunggu SPK Survey',
    'Menunggu RAB'),
   'Menunggu SPK Pemasangan'),
  'Menunggu Berita Acara'),
 'Selesai') AS `statuspermohonan`
FROM
`pendaftaran` p
LEFT JOIN __tmp_tipesambungan tp ON tp.tipe=p.`tipe`
LEFT JOIN [bsbs].rayon r ON r.koderayon = p.koderayon
LEFT JOIN [bsbs].kelurahan k ON k.kodekelurahan = p.kodekelurahan
LEFT JOIN `spk_opname_sambung_baru` spk ON spk.`nomorreg`=p.`nomorreg`
LEFT JOIN `rab` rab ON rab.`nomorreg`=p.`nomorreg`
,(SELECT @id := @lastid) AS id
WHERE p.`flaghapus`=0 AND spk.`flaghapus`=0 AND rab.`flaghapus`=0