-- idtipepermohonan	kodetipepermohonan
-- 17	DATA_PEMBAYARAN
-- 19	KOREKSI_PEMBACAAN
-- 33	UKURAN_METER
-- 40	INFO_DATA_PELANGGAN
-- 44	INFO_PEMBAYARAN
-- 49	INFO_TEKNIS

DELETE FROM `permohonan_pelanggan_air_spk_pasang` WHERE idpdam=@idpdam AND `idpermohonan` IN (
SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan` IN (17,19,33,40,44,49)
);

UPDATE `permohonan_pelanggan_air_ba` SET `flag_dari_verifikasi`=NULL,`statusberitaacara`=NULL WHERE idpdam=@idpdam AND `idpermohonan` IN (
SELECT `idpermohonan` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan` IN (17,19,33,40,44,49) AND `statuspermohonan`='Selesai'
);

REPLACE INTO `permohonan_pelanggan_air_ba` (idpdam,`idpermohonan`,`nomorba`,`tanggalba`,`iduser`)
SELECT `idpdam`,`idpermohonan`,'-',`waktupermohonan`,`iduser` FROM `permohonan_pelanggan_air` WHERE idpdam=@idpdam AND `idtipepermohonan` IN (17,19,33,40,44,49) AND `statuspermohonan`<>'Selesai';

REPLACE INTO `permohonan_pelanggan_air_ba_detail`
SELECT a.* FROM (
SELECT
p.`idpdam` AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Diameter Baru' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`waktupermohonan` AS `waktuupdate`
FROM `permohonan_pelanggan_air` p
WHERE p.`idpdam`=@idpdam AND p.`idtipepermohonan` IN (17,19,33,40,44,49) AND p.`statuspermohonan`<>'Selesai'
UNION ALL
SELECT
p.`idpdam` AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Keterangan Lapangan' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`waktupermohonan` AS `waktuupdate`
FROM `permohonan_pelanggan_air` p
WHERE p.`idpdam`=@idpdam AND p.`idtipepermohonan` IN (17,19,33,40,44,49) AND p.`statuspermohonan`<>'Selesai'
UNION ALL
SELECT
p.`idpdam` AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Merk Meter Baru' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`waktupermohonan` AS `waktuupdate`
FROM `permohonan_pelanggan_air` p
WHERE p.`idpdam`=@idpdam AND p.`idtipepermohonan` IN (17,19,33,40,44,49) AND p.`statuspermohonan`<>'Selesai'
UNION ALL
SELECT
p.`idpdam` AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Meteran Diganti' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`waktupermohonan` AS `waktuupdate`
FROM `permohonan_pelanggan_air` p
WHERE p.`idpdam`=@idpdam AND p.`idtipepermohonan` IN (17,19,33,40,44,49) AND p.`statuspermohonan`<>'Selesai'
UNION ALL
SELECT
p.`idpdam` AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'No Segel Baru' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`waktupermohonan` AS `waktuupdate`
FROM `permohonan_pelanggan_air` p
WHERE p.`idpdam`=@idpdam AND p.`idtipepermohonan` IN (17,19,33,40,44,49) AND p.`statuspermohonan`<>'Selesai'
UNION ALL
SELECT
p.`idpdam` AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Serimeter Baru' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`waktupermohonan` AS `waktuupdate`
FROM `permohonan_pelanggan_air` p
WHERE p.`idpdam`=@idpdam AND p.`idtipepermohonan` IN (17,19,33,40,44,49) AND p.`statuspermohonan`<>'Selesai'
UNION ALL
SELECT
p.`idpdam` AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Tanggal Pengerjaan Akhir' AS `parameter`,
'date' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`waktupermohonan` AS `waktuupdate`
FROM `permohonan_pelanggan_air` p
WHERE p.`idpdam`=@idpdam AND p.`idtipepermohonan` IN (17,19,33,40,44,49) AND p.`statuspermohonan`<>'Selesai'
UNION ALL
SELECT
p.`idpdam` AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Tanggal Pengerjaan Awal' AS `parameter`,
'date' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`waktupermohonan` AS `waktuupdate`
FROM `permohonan_pelanggan_air` p
WHERE p.`idpdam`=@idpdam AND p.`idtipepermohonan` IN (17,19,33,40,44,49) AND p.`statuspermohonan`<>'Selesai'
) a;

UPDATE `permohonan_pelanggan_air` SET `statuspermohonan`='Selesai' WHERE idpdam=@idpdam AND `idtipepermohonan` IN (17,19,33,40,44,49) AND `statuspermohonan`<>'Selesai';