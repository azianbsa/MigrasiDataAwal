﻿-- permohonan_pelanggan_air_spk_pasang
-- new(0, "idpdam")
-- new(1, "idpermohonan")
-- new(2, "nomorspk")
-- new(3, "tanggalspk")
-- new(4, "nomorsppb")
-- new(5, "tanggalsppb")
-- new(6, "iduser")
-- new(7, "flagbatal")
-- new(8, "idalasanbatal")
-- new(9, "waktuupdate")

SELECT
@idpdam AS `idpdam`,
pp.`idpermohonan` AS `idpermohonan`,
p.`nomorspk` AS `nomorspk`,
p.`tanggalspk` AS `tanggalspk`,
NULL AS `nomorsppb`,
NULL AS `tanggalsppb`,
u.iduser AS `iduser`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
p.`tanggalspk` AS `waktuupdate`
FROM `spk_bukasegel` p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.`nomorpermohonan`=p.`nomorpermohonan`
LEFT JOIN `kotaparepare_dataawal`.`master_user` u ON u.nama=p.user
WHERE p.flaghapus=0