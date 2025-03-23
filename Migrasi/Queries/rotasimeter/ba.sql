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

DROP TABLE IF EXISTS __tmp_rotasimeter;
CREATE TABLE __tmp_rotasimeter AS
SELECT
@id := @id+1 AS id,
p.nosamb,
p.periode
FROM `rotasimeter` p
,(SELECT @id := @lastid) AS id;

SELECT
@idpdam AS idpdam,
pp.`id` AS idpermohonan,
p.`no_ba` AS nomorba,
p.`tgl_ba` AS tanggalba,
u.iduser AS iduser,
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
NULL AS flag_dari_verifikasi,
'Berhasil Dikerjakan' AS statusberitaacara,
p.`tgl_ba` AS waktuupdate
FROM __tmp_rotasimeter pp
JOIN `rotasimeter` p ON p.`nosamb`=pp.`nosamb` AND p.`periode`=pp.`periode`
LEFT JOIN __tmp_userloket u ON u.nama=p.`user_ba`
WHERE p.`flag_ba`=1