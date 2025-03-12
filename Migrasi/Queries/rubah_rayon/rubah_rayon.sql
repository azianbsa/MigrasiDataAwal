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

DROP TEMPORARY TABLE IF EXISTS __tmp_rubahrayon;
CREATE TEMPORARY TABLE __tmp_rubahrayon AS
SELECT
@id:=@id+1 AS id,
nomor
FROM `permohonan_rubah_rayon`
,(SELECT @id:=@lastid) AS id
WHERE flaghapus=0;

SELECT
@idpdam as idpdam,
r.id AS idpermohonan,
@tipepermohonan AS idtipepermohonan,
NULL AS idsumberpengaduan,
p.nomor AS nomorpermohonan,
p.tanggal AS waktupermohonan,
ray.id AS idrayon,
kel.id AS idkelurahan,
gol.id AS idgolongan,
NULL AS iddiameter,
pel.id idpelangganair,
p.keterangan AS keterangan,
usr.iduser AS iduser,
NULL AS idnonair,
NULL AS latitude,
NULL AS longitude,
NULL AS alamatmap,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
IF(v.nomorba IS NULL,0,1) AS flagverifikasi,
v.waktuverifikasi AS waktuverifikasi,
0 AS flagusulan,
IF(v.nomorba IS NULL,
 IF(p.`flag_ba`=0,
  IF(p.`flag_spk`=0,
   'Menunggu SPK Pemasangan',
   'Menunggu Berita Acara'),
  'Menunggu Verifikasi'),
 'Selesai') AS statuspermohonan,
0 AS flaghapus,
p.tanggal AS waktuupdate
FROM __tmp_rubahrayon r
JOIN permohonan_rubah_rayon p ON p.nomor=r.nomor
JOIN pelanggan pel ON pel.nosamb=p.nosamb
LEFT JOIN `verifikasi` v ON v.nomorba=p.`nomor_ba`
LEFT JOIN [bsbs].rayon ray ON ray.koderayon=p.koderayon
LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan=p.kodekelurahan
LEFT JOIN __tmp_golongan gol ON gol.kodegol=p.kodegol AND gol.aktif=1
LEFT JOIN __tmp_userloket usr ON usr.nama=p.user_input