-- permohonan_pelanggan_air_spk
-- new(0, "idpdam")
-- new(1, "idpermohonan")
-- new(2, "nomorspk")
-- new(3, "tanggalspk")
-- new(4, "iduser")
-- new(5, "flagsurvey")
-- new(6, "flagbatal")
-- new(7, "idalasanbatal")
-- new(8, "waktuupdate")

SELECT
@idpdam AS `idpdam`,
pp.`idpermohonan` AS `idpermohonan`,
p.`nospk_pengecekan` AS `nomorspk`,
p.`tglspk_pengecekan` AS `tanggalspk`,
u.`iduser` AS `iduser`,
1 AS `flagsurvey`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
p.`tglspk_pengecekan` AS `waktuupdate`
FROM `permohonan_rubah_gol` p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.`nomorpermohonan`=p.`nomor`
LEFT JOIN `kotaparepare_dataawal`.`master_user` u ON u.nama=SUBSTRING_INDEX(p.urutannonair,'.RUBAH_GOL.',1)
WHERE p.`flag_spk_pengecekan`=1