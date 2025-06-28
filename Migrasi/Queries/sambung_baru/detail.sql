SET @tgl_reg_awal='2013-01-01';
SET @tgl_reg_akhir='2025-06-01';

SELECT
@idpdam AS `idpdam`,
b.idpermohonan AS `idpermohonan`,
'Denah Lokasi' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
0 AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_pelanggan_reg` a
JOIN `pelanggan_reg` b ON b.`no_reg`=a.`no_reg`
WHERE a.`no_reg` NOT IN ('`','-','s')
AND a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Ditagih Setelah' AS `parameter`,
'string' AS `tipedata`,
'' AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_pelanggan_reg` a
JOIN `pelanggan_reg` b ON b.`no_reg`=a.`no_reg`
WHERE a.`no_reg` NOT IN ('`','-','s')
AND a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Foto Copy IMB/Keterangan Lurah' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
0 AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_pelanggan_reg` a
JOIN `pelanggan_reg` b ON b.`no_reg`=a.`no_reg`
WHERE a.`no_reg` NOT IN ('`','-','s')
AND a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Foto Copy KK' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
1 AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_pelanggan_reg` a
JOIN `pelanggan_reg` b ON b.`no_reg`=a.`no_reg`
WHERE a.`no_reg` NOT IN ('`','-','s')
AND a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Foto Copy KTP' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
1 AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_pelanggan_reg` a
JOIN `pelanggan_reg` b ON b.`no_reg`=a.`no_reg`
WHERE a.`no_reg` NOT IN ('`','-','s')
AND a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Map Snelhecter Plastik' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
0 AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_pelanggan_reg` a
JOIN `pelanggan_reg` b ON b.`no_reg`=a.`no_reg`
WHERE a.`no_reg` NOT IN ('`','-','s')
AND a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Meterai 10.000' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
0 AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_pelanggan_reg` a
JOIN `pelanggan_reg` b ON b.`no_reg`=a.`no_reg`
WHERE a.`no_reg` NOT IN ('`','-','s')
AND a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Surat Pernyataan' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
0 AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_pelanggan_reg` a
JOIN `pelanggan_reg` b ON b.`no_reg`=a.`no_reg`
WHERE a.`no_reg` NOT IN ('`','-','s')
AND a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir