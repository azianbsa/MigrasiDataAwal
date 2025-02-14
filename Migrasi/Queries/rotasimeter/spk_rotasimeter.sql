﻿DROP TEMPORARY TABLE IF EXISTS __tmp_userbshl;
CREATE TEMPORARY TABLE __tmp_userbshl AS
SELECT
@iduser := @iduser + 1 AS iduser,
nama
FROM `userbshl`
,(SELECT @iduser := 0) AS iduser
ORDER BY nama;

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
LEFT JOIN __tmp_userbshl usr ON usr.nama = p.`user_spk`
WHERE p.`flag_spk`=1 AND p.`tgl_spk` IS NOT NULL