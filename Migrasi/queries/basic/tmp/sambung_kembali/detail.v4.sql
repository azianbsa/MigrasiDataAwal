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
FROM `permohonan_sambung_kembali` p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.`nomorpermohonan`=p.`nomor`