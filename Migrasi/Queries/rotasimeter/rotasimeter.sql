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
CAST(CONCAT(p.nosamb,'.',p.periode) AS CHAR) AS nomorpermohonan,
STR_TO_DATE(CONCAT(p.periode,'01'),'%Y%m%d') AS waktupermohonan,
r.id AS idrayon,
k.id AS idkelurahan,
g.id AS idgolongan,
NULL AS iddiameter,
pl.id idpelangganair,
'GANTI METER RUTIN' AS keterangan,
NULL AS iduser,
NULL AS idnonair,
NULL AS latitude,
NULL AS longitude,
NULL AS alamatmap,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
IF(p.tgl_ba IS NULL OR p.flag_ba=0,0,1) AS flagverifikasi,
IF(p.tgl_ba<>'0000-00-00' AND p.flag_ba=1,p.tgl_ba,NULL) AS waktuverifikasi,
0 AS flagusulan,
IF(p.tgl_ba IS NULL OR p.flag_ba=0,
 IF(p.`tgl_spk` IS NULL OR p.flag_spk=0,
  'Menunggu SPK Pemasangan',
  'Menunggu Berita Acara'),
 'Selesai') AS statuspermohonan,
0 AS flaghapus,
COALESCE(
 IF(p.tgl_ba<>'0000-00-00' AND p.flag_ba=1,p.tgl_ba,NULL),
 IF(p.tgl_spk<>'0000-00-00' AND p.flag_spk=1,p.tgl_spk,NULL),
 STR_TO_DATE(CONCAT(p.periode,'01'),'%Y%m%d')) waktuupdate
FROM `rotasimeter` p
JOIN pelanggan pl ON pl.nosamb = p.nosamb
LEFT JOIN [bsbs].rayon r ON r.koderayon = p.koderayon
LEFT JOIN [bsbs].kelurahan k ON k.kodekelurahan = p.kodekelurahan
LEFT JOIN __tmp_golongan g ON g.kodegol = p.kodegol AND g.aktif = 1
,(SELECT @id := @lastid) AS id