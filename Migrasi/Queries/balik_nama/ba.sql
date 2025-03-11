DROP TEMPORARY TABLE IF EXISTS __tmp_baliknama;
CREATE TEMPORARY TABLE __tmp_baliknama AS
SELECT
@id:=@id+1 AS id,
nomor
FROM `permohonan_balik_nama`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

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

SELECT
@idpdam AS idpdam,
b.id AS idpermohonan,
ba.nomorba AS nomorba,
ba.tanggalba AS tanggalba,
usr.iduser AS iduser,
NULL AS persilnamapaket,
0 AS persilflagdialihkankevendor,
0 AS persilflagbiayadibebankankepdam,
NULL AS distribusinamapaket,
0 AS distribusiflagdialihkankevendor,
0 AS distribusiflagbiayadibebankankepdam,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
NULL AS kategoriputus,
0 AS flagbatal,
NULL AS idalasanbatal,
1 AS flag_dari_verifikasi,
NULL AS statusberitaacara,
NOW() AS waktuupdate
FROM __tmp_baliknama b
JOIN ba_balik_nama ba ON ba.nomorpermohonan=b.nomor
JOIN permohonan_balik_nama bn ON bn.nomor=b.nomor
LEFT JOIN __tmp_userloket usr ON usr.nama=SUBSTRING_INDEX(bn.urutannonair,'.BALIK NAMA.',1)
WHERE ba.flaghapus=0