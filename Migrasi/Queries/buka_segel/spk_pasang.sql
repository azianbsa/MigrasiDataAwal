SELECT
@idpdam AS `idpdam`,
pp.`idpermohonan` AS `idpermohonan`,
p.`nomorspk` AS `nomorspk`,
p.`tanggalspk` AS `tanggalspk`,
NULL AS `nomorsppb`,
NULL AS `tanggalsppb`,
u.iduser AS `iduser`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
p.`tanggalspk` AS `waktuupdate`
FROM `spk_bukasegel` p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.`nomorpermohonan`=p.`nomorpermohonan`
LEFT JOIN `kotaparepare_dataawal`.`master_user` u ON u.nama=p.user
WHERE p.flaghapus=0