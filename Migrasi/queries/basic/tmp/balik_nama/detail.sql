SET @tgl_reg_awal='2017-01-01';
SET @tgl_reg_akhir='2025-07-01';

SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Alamat Pemilik Baru' AS `parameter`,
'string' AS `tipedata`,
'' AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_reg_bn` a
JOIN `baliknama` b ON b.`no_reg`=a.`no_reg`
WHERE a.tgl_reg>=@tgl_reg_awal
AND a.tgl_reg<@tgl_reg_akhir
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
FROM `t_reg_bn` a
JOIN `baliknama` b ON b.`no_reg`=a.`no_reg`
WHERE a.tgl_reg>=@tgl_reg_awal
AND a.tgl_reg<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Email Baru' AS `parameter`,
'string' AS `tipedata`,
'' AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_reg_bn` a
JOIN `baliknama` b ON b.`no_reg`=a.`no_reg`
WHERE a.tgl_reg>=@tgl_reg_awal
AND a.tgl_reg<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Kepemilikan Baru' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
1 AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_reg_bn` a
JOIN `baliknama` b ON b.`no_reg`=a.`no_reg`
WHERE a.tgl_reg>=@tgl_reg_awal
AND a.tgl_reg<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Nama Baru' AS `parameter`,
'string' AS `tipedata`,
a.`nama_baru` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_reg_bn` a
JOIN `baliknama` b ON b.`no_reg`=a.`no_reg`
WHERE a.tgl_reg>=@tgl_reg_awal
AND a.tgl_reg<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Nama Lama' AS `parameter`,
'string' AS `tipedata`,
a.`nama_lama` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_reg_bn` a
JOIN `baliknama` b ON b.`no_reg`=a.`no_reg`
WHERE a.tgl_reg>=@tgl_reg_awal
AND a.tgl_reg<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Nama Pemilik Baru' AS `parameter`,
'string' AS `tipedata`,
a.`nama_baru` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_reg_bn` a
JOIN `baliknama` b ON b.`no_reg`=a.`no_reg`
WHERE a.tgl_reg>=@tgl_reg_awal
AND a.tgl_reg<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'No HP Baru' AS `parameter`,
'string' AS `tipedata`,
a.`telp` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_reg_bn` a
JOIN `baliknama` b ON b.`no_reg`=a.`no_reg`
WHERE a.tgl_reg>=@tgl_reg_awal
AND a.tgl_reg<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'No KK Baru' AS `parameter`,
'string' AS `tipedata`,
'' AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_reg_bn` a
JOIN `baliknama` b ON b.`no_reg`=a.`no_reg`
WHERE a.tgl_reg>=@tgl_reg_awal
AND a.tgl_reg<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'No KTP Baru' AS `parameter`,
'string' AS `tipedata`,
'' AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_reg_bn` a
JOIN `baliknama` b ON b.`no_reg`=a.`no_reg`
WHERE a.tgl_reg>=@tgl_reg_awal
AND a.tgl_reg<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'No Telp Baru' AS `parameter`,
'string' AS `tipedata`,
a.`telp` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_reg_bn` a
JOIN `baliknama` b ON b.`no_reg`=a.`no_reg`
WHERE a.tgl_reg>=@tgl_reg_awal
AND a.tgl_reg<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Pekerjaan Baru' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
1 AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_reg_bn` a
JOIN `baliknama` b ON b.`no_reg`=a.`no_reg`
WHERE a.tgl_reg>=@tgl_reg_awal
AND a.tgl_reg<@tgl_reg_akhir