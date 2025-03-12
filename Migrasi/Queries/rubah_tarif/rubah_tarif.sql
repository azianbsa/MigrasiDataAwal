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

DROP TEMPORARY TABLE IF EXISTS __tmp_rubah_tarif;
CREATE TEMPORARY TABLE __tmp_rubah_tarif AS
SELECT
@id:=@id+1 AS id,
nomor
FROM `permohonan_rubah_gol`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

SELECT
@idpdam AS idpdam,
t.id AS idpermohonan,
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
p.flag_ba_pengecekan AS flagverifikasi,
p.tglba_pengecekan AS waktuverifikasi,
0 AS flagusulan,
IF(p.flag_ba_pengecekan=1,
 'Selesai',
 IF(p.flag_spk_pengecekan=1,
  'Menunggu Verifikasi',
  'Menunggu SPK Survey')) AS statuspermohonan,
0 AS flaghapus,
p.tanggal waktuupdate
FROM __tmp_rubah_tarif t
JOIN permohonan_rubah_gol p ON p.nomor=t.nomor
JOIN pelanggan pel ON pel.nosamb=p.nosamb
LEFT JOIN [bsbs].rayon ray ON ray.koderayon=p.koderayon
LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan=p.kodekelurahan
LEFT JOIN __tmp_golongan gol ON gol.kodegol=p.kodegol AND gol.aktif=1
LEFT JOIN __tmp_userloket usr ON usr.nama=SUBSTRING_INDEX(p.urutannonair,'.RUBAH_GOL.',1)