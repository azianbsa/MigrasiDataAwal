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

DROP TEMPORARY TABLE IF EXISTS __tmp_rubahrayon;
CREATE TEMPORARY TABLE __tmp_rubahrayon AS
SELECT
@id:=@id+1 AS id,
nomor
FROM `permohonan_rubah_rayon`
,(SELECT @id:=@lastid) AS id
WHERE flaghapus=0;

SELECT
@idpdam AS idpdam,
p.id AS idpermohonan,
pp.nomor_ba AS nomorba,
pp.tanggal_ba AS tanggalba,
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
NULL AS `fotobukti4`,
NULL AS `fotobukti5`,
NULL AS `fotobukti6`,
NULL AS kategoriputus,
0 AS flagbatal,
NULL AS idalasanbatal,
NULL AS flag_dari_verifikasi,
'Berhasil Dikerjakan' AS statusberitaacara,
pp.`tanggal_ba` AS waktuupdate
FROM __tmp_rubahrayon p
JOIN permohonan_rubah_rayon pp ON pp.nomor=p.nomor
LEFT JOIN __tmp_userloket usr ON usr.nama=pp.user_ba
WHERE pp.`flag_ba`=1