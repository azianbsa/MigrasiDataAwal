SET @tgl_reg_awal='2002-01-01';
SET @tgl_reg_akhir='2019-01-01';

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
a.`tgl_sm` AS `waktuupdate`
FROM `t_reg_sm` a
JOIN `segeltunggakan` g ON g.`no_sm`=a.`no_sm`
WHERE a.`tgl_sm`>=@tgl_reg_awal
AND a.`tgl_sm`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
g.`idpermohonan` AS `idpermohonan`,
'Mau Bayar' AS `parameter`,
'string' AS `tipedata`,
'Mau Bayar' AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_sm` AS `waktuupdate`
FROM `t_reg_sm` a
JOIN `segeltunggakan` g ON g.`no_sm`=a.`no_sm`
WHERE a.`tgl_sm`>=@tgl_reg_awal
AND a.`tgl_sm`<@tgl_reg_akhir
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
a.`tgl_sm` AS `waktuupdate`
FROM `t_reg_sm` a
JOIN `segeltunggakan` g ON g.`no_sm`=a.`no_sm`
WHERE a.`tgl_sm`>=@tgl_reg_awal
AND a.`tgl_sm`<@tgl_reg_akhir
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
a.`tgl_sm` AS `waktuupdate`
FROM `t_reg_sm` a
JOIN `segeltunggakan` g ON g.`no_sm`=a.`no_sm`
LEFT JOIN `kabmaros_loket4`.`pelanggan` p ON p.nosamb=a.`nosamb`
WHERE a.`tgl_sm`>=@tgl_reg_awal
AND a.`tgl_sm`<@tgl_reg_akhir
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
a.`tgl_sm` AS `waktuupdate`
FROM `t_reg_sm` a
JOIN `segeltunggakan` g ON g.`no_sm`=a.`no_sm`
WHERE a.`tgl_sm`>=@tgl_reg_awal
AND a.`tgl_sm`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
g.`idpermohonan` AS `idpermohonan`,
'Warna Segel' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
1 AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_sm` AS `waktuupdate`
FROM `t_reg_sm` a
JOIN `segeltunggakan` g ON g.`no_sm`=a.`no_sm`
WHERE a.`tgl_sm`>=@tgl_reg_awal
AND a.`tgl_sm`<@tgl_reg_akhir