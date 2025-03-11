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

DROP TEMPORARY TABLE IF EXISTS __tmp_baliknama;
CREATE TEMPORARY TABLE __tmp_baliknama AS
SELECT
@id:=@id+1 AS id,
nomor
FROM `permohonan_balik_nama`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

SELECT
@idpdam as idpdam,
b.id AS idpermohonan,
@tipepermohonan AS idtipepermohonan,
NULL AS idsumberpengaduan,
bn.nomor AS nomorpermohonan,
bn.tanggal AS waktupermohonan,
ray.id AS idrayon,
kel.id AS idkelurahan,
gol.id AS idgolongan,
NULL AS iddiameter,
pel.id idpelangganair,
bn.keterangan AS keterangan,
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
IF(ba.nomorba IS NULL,
 'Menunggu Verifikasi',
 'Selesai') AS statuspermohonan,
0 AS flaghapus,
bn.tanggal waktuupdate
FROM __tmp_baliknama b
JOIN permohonan_balik_nama bn ON bn.nomor=b.nomor
JOIN pelanggan pel ON pel.nosamb=bn.nosamb
LEFT JOIN [bsbs].rayon ray ON ray.koderayon=bn.koderayon
LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan=bn.kodekelurahan
LEFT JOIN __tmp_golongan gol ON gol.kodegol=bn.kodegol AND gol.aktif=1
LEFT JOIN ba_balik_nama ba ON ba.nomorpermohonan=bn.nomor
LEFT JOIN __tmp_userloket usr ON usr.nama=SUBSTRING_INDEX(bn.urutannonair,'.BALIK NAMA.',1)
WHERE ba.flaghapus=0