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
@idpdam AS `idpdam`,
r.`idpermohonan` AS `idpermohonan`,
p.`no_spk` AS `nomorspk`,
p.`tgl_spk` AS `tanggalspk`,
usr.iduser AS `iduser`,
1 AS `flagsurvey`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
p.`tgl_spk` AS `waktuupdate`
FROM
`rotasimeter` p
JOIN __tmp_rotasimeter r ON r.`nosamb`=p.`nosamb` AND r.`periode`=p.`periode`
LEFT JOIN __tmp_userloket usr ON usr.nama = p.`user_spk`
WHERE p.`flag_spk`=1 AND p.`tgl_spk` IS NOT NULL