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
p.`nomorspk` AS `nomorspk`,
p.`tanggalspk` AS `tanggalspk`,
u.iduser AS `iduser`,
1 AS `flagsurvey`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
p.`tanggalspk` AS `waktuupdate`
FROM spk_pemutusan_sementara p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.nomorpermohonan=p.`nomorpermohonan`
LEFT JOIN `kotaparepare_dataawal`.`master_user` u ON u.nama=p.user
WHERE p.flaghapus=0