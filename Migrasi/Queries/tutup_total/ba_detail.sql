SET @tgl_reg_awal='2025-06-26';
SET @tgl_reg_akhir='2025-06-29';

SELECT
@idpdam AS `idpdam`,
g.`idpermohonan` AS `idpermohonan`,
'Angka Meter' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
a.`met_akh` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_cm` AS `waktuupdate`
FROM `t_reg_cm` a
JOIN `tutuptotaltunggakan` g ON g.`no_cm`=a.`no_cm`
WHERE a.`tgl_cm`>=@tgl_reg_awal
AND a.`tgl_cm`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
g.`idpermohonan` AS `idpermohonan`,
'Merk Meter' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
-1 AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_cm` AS `waktuupdate`
FROM `t_reg_cm` a
JOIN `tutuptotaltunggakan` g ON g.`no_cm`=a.`no_cm`
WHERE a.`tgl_cm`>=@tgl_reg_awal
AND a.`tgl_cm`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
g.`idpermohonan` AS `idpermohonan`,
'Seri Meter' AS `parameter`,
'string' AS `tipedata`,
COALESCE(p.`serimeter`,'-') AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_cm` AS `waktuupdate`
FROM `t_reg_cm` a
JOIN `tutuptotaltunggakan` g ON g.`no_cm`=a.`no_cm`
LEFT JOIN `kabmaros_loket4`.`pelanggan` p ON p.nosamb=a.`nosamb`
WHERE a.`tgl_cm`>=@tgl_reg_awal
AND a.`tgl_cm`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
g.`idpermohonan` AS `idpermohonan`,
'Seri Segel' AS `parameter`,
'string' AS `tipedata`,
'-' AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_cm` AS `waktuupdate`
FROM `t_reg_cm` a
JOIN `tutuptotaltunggakan` g ON g.`no_cm`=a.`no_cm`
WHERE a.`tgl_cm`>=@tgl_reg_awal
AND a.`tgl_cm`<@tgl_reg_akhir