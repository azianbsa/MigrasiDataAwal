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

SELECT
@idpdam as idpdam,
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
usr.iduser AS iduser,
NULL AS idnonair,
NULL AS latitude,
NULL AS longitude,
NULL AS alamatmap,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
IF(per.nomor_ba IS NULL,0,1) AS flagverifikasi,
per.tanggal_ba AS waktuverifikasi,
0 AS flagusulan,
IF(per.flag_ba=0,
 IF(per.flag_spk=0,
  'Menunggu SPK Pemasangan',
  'Menunggu Berita Acara'),
 'Selesai') AS statuspermohonan,
0 AS flaghapus,
per.tanggal waktuupdate
FROM permohonan_rubah_rayon per
JOIN pelanggan pel ON pel.nosamb = per.nosamb
LEFT JOIN [bsbs].rayon ray ON ray.koderayon = per.koderayon
LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan = per.kodekelurahan
LEFT JOIN __tmp_golongan gol ON gol.kodegol = per.kodegol AND gol.aktif = 1
LEFT JOIN __tmp_userloket usr ON usr.nama = per.user_input
,(SELECT @id := @lastid) AS id
WHERE per.flaghapus = 0