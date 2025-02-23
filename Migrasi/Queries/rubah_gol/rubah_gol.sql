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
@idpdam AS idpdam,
@id := @id+1 AS idpermohonan,
@tipepermohonan AS idtipepermohonan,
NULL AS idsumberpengaduan,
rg.nomor AS nomorpermohonan,
rg.tanggal AS waktupermohonan,
ray.id AS idrayon,
kel.id AS idkelurahan,
gol.id AS idgolongan,
NULL AS iddiameter,
pel.id idpelangganair,
rg.keterangan AS keterangan,
usr.iduser AS iduser,
NULL AS idnonair,
NULL AS latitude,
NULL AS longitude,
NULL AS alamatmap,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
IF(ba.nomorba IS NULL,0,1) AS flagverifikasi,
ba.tanggalba AS waktuverifikasi,
0 AS flagusulan,
IF(rg.flag_ba_pengecekan=1,
 'Selesai',
 IF(rg.flag_spk_pengecekan=1,
  'Menunggu Berita Acara',
  'Menunggu SPK Survey')) AS statuspermohonan,
0 AS flaghapus,
rg.tanggal waktuupdate
FROM permohonan_rubah_gol rg
JOIN pelanggan pel ON pel.nosamb = rg.nosamb
LEFT JOIN [bsbs].rayon ray ON ray.koderayon = rg.koderayon
LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan = rg.kodekelurahan
LEFT JOIN __tmp_golongan gol ON gol.kodegol = rg.kodegol AND gol.aktif = 1
LEFT JOIN ba_rubah_gol ba ON ba.nomorpermohonan = rg.nomor AND ba.flaghapus=0
LEFT JOIN __tmp_userloket usr ON usr.nama = SUBSTRING_INDEX(rg.urutannonair,'.RUBAH_GOL.',1)
,(SELECT @id := @lastid) AS id
WHERE rg.flaghapus = 0