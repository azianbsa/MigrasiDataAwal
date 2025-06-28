SET @tgl_reg_awal='2013-01-01';
SET @tgl_reg_akhir='2025-06-01';

SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Diameter' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
d.`iddiameter` AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_spko` AS waktuupdate
FROM `t_pelanggan_reg` a
JOIN `pelanggan_reg` b ON b.`no_reg`=a.`no_reg`
LEFT JOIN `diametermaros` d ON d.`kodediameter`=a.`dia_met`
WHERE a.`no_reg` NOT IN ('`','-','s')
AND a.`no_spko`<>'-'
AND a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Diameter Distribusi' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
d.`iddiameter` AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_spko` AS waktuupdate
FROM `t_pelanggan_reg` a
JOIN `pelanggan_reg` b ON b.`no_reg`=a.`no_reg`
LEFT JOIN `diametermaros` d ON d.`kodediameter`=a.`dia_met`
WHERE a.`no_reg` NOT IN ('`','-','s')
AND a.`no_spko`<>'-'
AND a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Golongan' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
COALESCE(g.`idgolongan`,30) AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_spko` AS waktuupdate
FROM `t_pelanggan_reg` a
JOIN `pelanggan_reg` b ON b.`no_reg`=a.`no_reg`
LEFT JOIN `golonganmaros` g ON g.kodegolongan=a.`KDGT`
WHERE a.`no_reg` NOT IN ('`','-','s')
AND a.`no_spko`<>'-'
AND a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Jarak Pipa Distribusi (Meter)' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
0 AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_spko` AS waktuupdate
FROM `t_pelanggan_reg` a
JOIN `pelanggan_reg` b ON b.`no_reg`=a.`no_reg`
WHERE a.`no_reg` NOT IN ('`','-','s')
AND a.`no_spko`<>'-'
AND a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Keterangan Survey' AS `parameter`,
'string' AS `tipedata`,
'' AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_spko` AS waktuupdate
FROM `t_pelanggan_reg` a
JOIN `pelanggan_reg` b ON b.`no_reg`=a.`no_reg`
WHERE a.`no_reg` NOT IN ('`','-','s')
AND a.`no_spko`<>'-'
AND a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Peruntukan' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
COALESCE(bg.`idperuntukan`,1) AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_spko` AS waktuupdate
FROM `t_pelanggan_reg` a
JOIN `pelanggan_reg` b ON b.`no_reg`=a.`no_reg`
LEFT JOIN `t_fg_bgn` bg ON bg.`kd_fg_bgn`=a.`fgbgn`
WHERE a.`no_reg` NOT IN ('`','-','s')
AND a.`no_spko`<>'-'
AND a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Tanggal Realisasi Survey' AS `parameter`,
'date' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
a.`tgl_spko` AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_spko` AS waktuupdate
FROM `t_pelanggan_reg` a
JOIN `pelanggan_reg` b ON b.`no_reg`=a.`no_reg`
WHERE a.`no_reg` NOT IN ('`','-','s')
AND a.`no_spko`<>'-'
AND a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir