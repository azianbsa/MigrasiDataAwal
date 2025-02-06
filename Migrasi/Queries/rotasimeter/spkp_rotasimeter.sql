DROP TEMPORARY TABLE IF EXISTS __tmp_userbshl;
CREATE TEMPORARY TABLE __tmp_userbshl AS
SELECT
@iduser := @iduser + 1 AS iduser,
nama
FROM `userbshl`
,(SELECT @iduser := 0) AS iduser
ORDER BY nama;

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
`rotasimeter` p
JOIN __tmp_rotasimeter ro ON ro.`nosamb`=p.`nosamb` AND ro.`periode`=p.`periode`
LEFT JOIN __tmp_userbshl u ON u.nama=p.`user_spk`
WHERE p.`tgl_spk` IS NOT NULL AND p.`flag_spk`=1