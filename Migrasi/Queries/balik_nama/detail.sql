-- permohonan_pelanggan_air_detail
-- new(0, "idpdam")
-- new(1, "idpermohonan")
-- new(2, "parameter")
-- new(3, "tipedata")
-- new(4, "valuestring")
-- new(5, "valuedecimal")
-- new(6, "valueinteger")
-- new(7, "valuedate")
-- new(8, "valuebool")
-- new(9, "waktuupdate")

SET @idtipepermohonan=(SELECT idtipepermohonan FROM [dataawal].`master_attribute_tipe_permohonan` WHERE idpdam=@idpdam AND `kodetipepermohonan`='BALIK_NAMA');

SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Alamat Pemilik Baru' AS `parameter`,
'string' AS `tipedata`,
p.`alamat` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`tanggal` AS `waktuupdate`
FROM permohonan_balik_nama p
JOIN [dataawal].`tampung_permohonan_pelanggan_air` b ON b.`nomorpermohonan`=p.nomor AND b.`idtipepermohonan`=@idtipepermohonan AND b.`idpdam`=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Ditagih Setelah' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`tanggal` AS `waktuupdate`
FROM permohonan_balik_nama p
JOIN [dataawal].`tampung_permohonan_pelanggan_air` b ON b.`nomorpermohonan`=p.nomor AND b.`idtipepermohonan`=@idtipepermohonan AND b.`idpdam`=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Email Baru' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`tanggal` AS `waktuupdate`
FROM permohonan_balik_nama p
JOIN [dataawal].`tampung_permohonan_pelanggan_air` b ON b.`nomorpermohonan`=p.nomor AND b.`idtipepermohonan`=@idtipepermohonan AND b.`idpdam`=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Kepemilikan Baru' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
-1 AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`tanggal` AS `waktuupdate`
FROM permohonan_balik_nama p
JOIN [dataawal].`tampung_permohonan_pelanggan_air` b ON b.`nomorpermohonan`=p.nomor AND b.`idtipepermohonan`=@idtipepermohonan AND b.`idpdam`=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Nama Baru' AS `parameter`,
'string' AS `tipedata`,
p.`baru` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`tanggal` AS `waktuupdate`
FROM permohonan_balik_nama p
JOIN [dataawal].`tampung_permohonan_pelanggan_air` b ON b.`nomorpermohonan`=p.nomor AND b.`idtipepermohonan`=@idtipepermohonan AND b.`idpdam`=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Nama Lama' AS `parameter`,
'string' AS `tipedata`,
p.`lama` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`tanggal` AS `waktuupdate`
FROM permohonan_balik_nama p
JOIN [dataawal].`tampung_permohonan_pelanggan_air` b ON b.`nomorpermohonan`=p.nomor AND b.`idtipepermohonan`=@idtipepermohonan AND b.`idpdam`=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Nama Pemilik Baru' AS `parameter`,
'string' AS `tipedata`,
p.`baru` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`tanggal` AS `waktuupdate`
FROM permohonan_balik_nama p
JOIN [dataawal].`tampung_permohonan_pelanggan_air` b ON b.`nomorpermohonan`=p.nomor AND b.`idtipepermohonan`=@idtipepermohonan AND b.`idpdam`=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'No HP Baru' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`tanggal` AS `waktuupdate`
FROM permohonan_balik_nama p
JOIN [dataawal].`tampung_permohonan_pelanggan_air` b ON b.`nomorpermohonan`=p.nomor AND b.`idtipepermohonan`=@idtipepermohonan AND b.`idpdam`=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'No KK Baru' AS `parameter`,
'string' AS `tipedata`,
p.`nokk_baru` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`tanggal` AS `waktuupdate`
FROM permohonan_balik_nama p
JOIN [dataawal].`tampung_permohonan_pelanggan_air` b ON b.`nomorpermohonan`=p.nomor AND b.`idtipepermohonan`=@idtipepermohonan AND b.`idpdam`=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'No KTP Baru' AS `parameter`,
'string' AS `tipedata`,
p.`noktp_baru` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`tanggal` AS `waktuupdate`
FROM permohonan_balik_nama p
JOIN [dataawal].`tampung_permohonan_pelanggan_air` b ON b.`nomorpermohonan`=p.nomor AND b.`idtipepermohonan`=@idtipepermohonan AND b.`idpdam`=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'No Telp Baru' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`tanggal` AS `waktuupdate`
FROM permohonan_balik_nama p
JOIN [dataawal].`tampung_permohonan_pelanggan_air` b ON b.`nomorpermohonan`=p.nomor AND b.`idtipepermohonan`=@idtipepermohonan AND b.`idpdam`=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
'Pekerjaan Baru' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
COALESCE(pk.idpekerjaan,-1) AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`tanggal` AS `waktuupdate`
FROM permohonan_balik_nama p
JOIN [dataawal].`tampung_permohonan_pelanggan_air` b ON b.`nomorpermohonan`=p.nomor AND b.`idtipepermohonan`=@idtipepermohonan AND b.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_pekerjaan` pk ON pk.namapekerjaan=p.`pekerjaan_baru` AND pk.idpdam=@idpdam