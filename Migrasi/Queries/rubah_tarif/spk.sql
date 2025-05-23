SELECT
@idpdam AS `idpdam`,
pp.`idpermohonan` AS `idpermohonan`,
p.`nospk_pengecekan` AS `nomorspk`,
p.`tglspk_pengecekan` AS `tanggalspk`,
u.`iduser` AS `iduser`,
1 AS `flagsurvey`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
p.`tglspk_pengecekan` AS `waktuupdate`
FROM `permohonan_rubah_gol` p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.`nomorpermohonan`=p.`nomor`
LEFT JOIN `kotaparepare_dataawal`.`master_user` u ON u.nama=SUBSTRING_INDEX(p.urutannonair,'.RUBAH_GOL.',1)
WHERE p.`flag_spk_pengecekan`=1