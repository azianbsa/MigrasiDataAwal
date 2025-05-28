SELECT
@idpdam AS `idpdam`,
pp.`idpermohonan` AS `idpermohonan`,
p.`nomorspkopname` AS `nomorspk`,
p.`tglspko` AS `tanggalspk`,
u.iduser AS `iduser`,
p.`disetujui` AS `flagsurvey`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
p.`tglspko` AS `waktuupdate`
FROM `spk_opname_sambung_kembali` p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.nomorpermohonan=p.`nomorpermohonan`
LEFT JOIN `kotaparepare_dataawal`.`master_user` u ON u.`nama`=p.`user`
WHERE p.`flaghapus`