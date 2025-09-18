-- permohonan_non_pelanggan_detail
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

SET @idtipepermohonan=(SELECT idtipepermohonan FROM [dataawal].`master_attribute_tipe_permohonan` WHERE idpdam=@idpdam AND `kodetipepermohonan`='SAMBUNGAN_BARU_AIR');

SELECT
@idpdam AS `idpdam`,
p.idpermohonan AS `idpermohonan`,
'Denah Lokasi' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
0 AS `valuebool`,
NOW() AS `waktuupdate`
FROM [dataawal].`tampung_permohonan_non_pelanggan` p
WHERE p.idtipepermohonan=@idtipepermohonan AND p.idpdam=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Ditagih Setelah' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM [dataawal].`tampung_permohonan_non_pelanggan` p
WHERE p.idtipepermohonan=@idtipepermohonan AND p.idpdam=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Foto Copy IMB/Keterangan Lurah' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
0 AS `valuebool`,
NOW() AS `waktuupdate`
FROM [dataawal].`tampung_permohonan_non_pelanggan` p
WHERE p.idtipepermohonan=@idtipepermohonan AND p.idpdam=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Foto Copy KK' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
1 AS `valuebool`,
NOW() AS `waktuupdate`
FROM [dataawal].`tampung_permohonan_non_pelanggan` p
WHERE p.idtipepermohonan=@idtipepermohonan AND p.idpdam=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Foto Copy KTP' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
1 AS `valuebool`,
NOW() AS `waktuupdate`
FROM [dataawal].`tampung_permohonan_non_pelanggan` p
WHERE p.idtipepermohonan=@idtipepermohonan AND p.idpdam=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Map Snelhecter Plastik' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
0 AS `valuebool`,
NOW() AS `waktuupdate`
FROM [dataawal].`tampung_permohonan_non_pelanggan` p
WHERE p.idtipepermohonan=@idtipepermohonan AND p.idpdam=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Meterai 10.000' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
0 AS `valuebool`,
NOW() AS `waktuupdate`
FROM [dataawal].`tampung_permohonan_non_pelanggan` p
WHERE p.idtipepermohonan=@idtipepermohonan AND p.idpdam=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Surat Pernyataan' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
0 AS `valuebool`,
NOW() AS `waktuupdate`
FROM [dataawal].`tampung_permohonan_non_pelanggan` p
WHERE p.idtipepermohonan=@idtipepermohonan AND p.idpdam=@idpdam