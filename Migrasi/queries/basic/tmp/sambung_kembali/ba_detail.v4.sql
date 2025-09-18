-- permohonan_pelanggan_air_ba_detail
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

SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Keterangan' AS `parameter`,
'string' AS `tipedata`,
r.`memo` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
r.`tanggalba` AS `waktuupdate`
FROM `ba_sambungkembali` r
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` p ON p.`nomorpermohonan`=r.`nomorpermohonan`
WHERE r.`flaghapus`=0
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Kondisi Meter' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
-1 AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
r.`tanggalba` AS `waktuupdate`
FROM `ba_sambungkembali` r
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` p ON p.`nomorpermohonan`=r.`nomorpermohonan`
WHERE r.`flaghapus`=0
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Merek Meter' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
m.`idmerekmeter` AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
r.`tanggalba` AS `waktuupdate`
FROM `ba_sambungkembali` r
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` p ON p.`nomorpermohonan`=r.`nomorpermohonan`
LEFT JOIN `kotaparepare_dataawal`.`master_attribute_merek_meter` m ON m.namamerekmeter=r.`merkmeter`
WHERE r.`flaghapus`=0
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'No Segel' AS `parameter`,
'string' AS `tipedata`,
r.`nosegelmeter` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
r.`tanggalba` AS `waktuupdate`
FROM `ba_sambungkembali` r
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` p ON p.`nomorpermohonan`=r.`nomorpermohonan`
WHERE r.`flaghapus`=0
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Seri Meter' AS `parameter`,
'string' AS `tipedata`,
r.`serimeter` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
r.`tanggalba` AS `waktuupdate`
FROM `ba_sambungkembali` r
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` p ON p.`nomorpermohonan`=r.`nomorpermohonan`
WHERE r.`flaghapus`=0
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`idpermohonan` AS `idpermohonan`,
'Stan Meter' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
r.`angkameter` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
r.`tanggalba` AS `waktuupdate`
FROM `ba_sambungkembali` r
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` p ON p.`nomorpermohonan`=r.`nomorpermohonan`
WHERE r.`flaghapus`=0