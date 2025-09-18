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

SET @idtipepermohonan=(SELECT idtipepermohonan FROM `kotaparepare_dataawal`.`master_attribute_tipe_permohonan` WHERE idpdam=@idpdam AND `kodetipepermohonan`='BUKA_SEGEL');

SELECT
@idpdam AS `idpdam`,
pp.`idpermohonan` AS `idpermohonan`,
'Bagian' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`tanggal` AS `waktuupdate`
FROM `permohonan_bukasegel` p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.`nomorpermohonan`=p.`nomor` AND pp.`idtipepermohonan`=@idtipepermohonan AND pp.`idpdam`=@idpdam
UNION ALL
SELECT
@idpdam AS `idpdam`,
pp.`idpermohonan` AS `idpermohonan`,
'Ditagih Setelah' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
p.`tanggal` AS `waktuupdate`
FROM `permohonan_bukasegel` p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.`nomorpermohonan`=p.`nomor` AND pp.`idtipepermohonan`=@idtipepermohonan AND pp.`idpdam`=@idpdam