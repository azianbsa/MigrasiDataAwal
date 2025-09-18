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
@id:=@id+1 AS idpermohonan,
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
IF(v.`nomorba` IS NULL,0,1) AS flagverifikasi,
v.`waktuverifikasi` AS waktuverifikasi,
0 AS flagusulan,
IF(v.`nomorba` IS NULL,
 IF(p.`flag_ba`=0,
  IF(p.flag_spk=0,
   'Menunggu SPK Pemasangan',
   'Menunggu Berita Acara'),
  'Menunggu Verifikasi'),
 'Selesai') AS statuspermohonan,
0 AS flaghapus,
COALESCE(
 v.`waktuverifikasi`,
 p.`tgl_ba`,
 p.`tgl_spk`,
 STR_TO_DATE(CONCAT(p.periode,'01'),'%Y%m%d')) waktuupdate
FROM `rotasimeter` p
JOIN pelanggan pl ON pl.nosamb=p.nosamb
LEFT JOIN [bsbs].rayon r ON r.koderayon=p.koderayon
LEFT JOIN [bsbs].kelurahan k ON k.kodekelurahan=p.kodekelurahan
LEFT JOIN __tmp_golongan g ON g.kodegol=p.kodegol AND g.aktif=1
LEFT JOIN `verifikasi` v ON v.`nomorba`=p.`no_ba`
,(SELECT @id := @lastid) AS id