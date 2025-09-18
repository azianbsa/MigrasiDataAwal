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
pp.`idpermohonan` AS `idpermohonan`,
'Alasan Putus' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`tanggalba` AS `waktuupdate`
FROM `ba_bukasegel` p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.`nomorpermohonan`=p.`nomorpermohonan`
UNION ALL
SELECT
@idpdam AS `idpdam`,
pp.`idpermohonan` AS `idpermohonan`,
'Keterangan' AS `parameter`,
'string' AS `tipedata`,
p.`memo` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`tanggalba` AS `waktuupdate`
FROM `ba_bukasegel` p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.`nomorpermohonan`=p.`nomorpermohonan`