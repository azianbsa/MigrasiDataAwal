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
p.`nomorspkopname` AS `nomorspk`,
p.`tglspko` AS `tanggalspk`,
u.iduser AS `iduser`,
p.`disetujui` AS `flagsurvey`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
p.`tglspko` AS `waktuupdate`
FROM `spk_opname_sambung_kembali` p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.nomorpermohonan=p.`nomorpermohonan`
LEFT JOIN `kotaparepare_dataawal`.`master_user` u ON u.`nama`=p.`user`
WHERE p.`flaghapus`