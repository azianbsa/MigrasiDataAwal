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
ro.`idpermohonan` AS `idpermohonan`,
p.`no_spk` AS `nomorspk`,
p.`tgl_spk` AS `tanggalspk`,
NULL AS `nomorsppb`,
NULL AS `tanggalsppb`,
u.iduser AS `iduser`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
p.`tgl_spk` AS `waktuupdate`
FROM
`rotasimeter_nonrutin` p
JOIN __tmp_rotasimeter ro ON ro.`nosamb`=p.`nosamb` AND ro.`periode`=p.`periode`
LEFT JOIN __tmp_userloket u ON u.nama=p.`user_spk`
WHERE p.`tgl_spk` IS NOT NULL AND p.`flag_spk`=1