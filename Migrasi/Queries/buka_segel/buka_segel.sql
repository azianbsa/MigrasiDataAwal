DROP TEMPORARY TABLE IF EXISTS __tmp_golongan;
CREATE TEMPORARY TABLE __tmp_golongan AS
SELECT
@id:=@id+1 AS id,
kodegol,
aktif
FROM
golongan,
(SELECT @id:=0) AS id;

SELECT
@idpdam AS idpdam,
@id := @id+1 AS idpermohonan,
@tipepermohonan AS idtipepermohonan,
NULL AS idsumberpengaduan,
per.nomor AS nomorpermohonan,
per.tanggal AS waktupermohonan,
ray.id AS idrayon,
kel.id AS idkelurahan,
gol.id AS idgolongan,
NULL AS iddiameter,
pel.id idpelangganair,
per.keterangan AS keterangan,
NULL AS iduser,
na.id AS idnonair,
NULL AS latitude,
NULL AS longitude,
NULL AS alamatmap,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
IF(ba.tanggalba IS NULL,0,1) AS flagverifikasi,
ba.tanggalba AS waktuverifikasi,
0 AS flagusulan,
IF(ba.tanggalba IS NULL,
 IF(spk.tanggalspk IS NULL,
  IF(na.id IS NOT NULL AND na.waktubayar IS NULL,
   'Menunggu Pelunasan Reguler',
   'Menunggu SPK Pemasangan'),
  'Menunggu Berita Acara'),
 'Selesai') AS statuspermohonan,
0 AS flaghapus,
NULL AS waktuupdate
FROM `permohonan_bukasegel` per
JOIN pelanggan pel ON pel.nosamb = per.nosamb
LEFT JOIN [bsbs].rayon ray ON ray.koderayon = per.koderayon
LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan = per.kodekelurahan
LEFT JOIN __tmp_golongan gol ON gol.kodegol = per.kodegol AND gol.aktif = 1
LEFT JOIN nonair na ON na.urutan=per.urutannonair
LEFT JOIN `spk_bukasegel` spk ON spk.nomorpermohonan=per.nomor
LEFT JOIN `ba_bukasegel` ba ON ba.nomorpermohonan=per.nomor
,(SELECT @id := @lastid) AS id
WHERE per.flaghapus = 0 AND spk.flaghapus=0 AND ba.flaghapus=0