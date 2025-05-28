SET @idjenisnonair=(SELECT `idjenisnonair` FROM `kotaparepare_dataawal`.`master_attribute_jenis_nonair` WHERE idpdam=@idpdam AND `namajenisnonair`='SAMB.KEMBALI');

SELECT
@idpdam AS `idpdam`,
pp.`idpermohonan` AS `idpermohonan`,
@idjenisnonair AS `idjenisnonair`,
n.`idnonair` AS `idnonair`,
p.`norab` AS `nomorrab`,
p.`tglrab` AS `tanggalrab`,
REPLACE(p.`norab`,'/RAB SAMB.KEMBALI/','/BPPI SAMB.KEMBALI/') AS `nomorbppi`,
NULL AS `iduserbppi`,
p.`tglrab` AS `tanggalbppi`,
u.iduser AS `iduser`,
p.`validdate` AS `tanggalkadaluarsa`,
p.`namapaketrab` AS  `persilnamapaket`,
IFNULL(p.`dialihkankevendor`,0) AS `persilflagdialihkankevendor`,
IFNULL(p.`biayadibebankankepdam`,0) AS `persilflagbiayadibebankankepdam`,
0 AS `persilsubtotal`,
0 AS `persildibebankankepdam`,
0 AS `persiltotal`,
NULL AS `distribusinamapaket`,
0 AS `distribusiflagdialihkankevendor`,
0 AS `distribusiflagbiayadibebankankepdam`,
0 AS `distribusisubtotal`,
0 AS `distribusidibebankankepdam`,
0 AS `distribusitotal`,
NULL AS `fotodenah1`,
NULL AS `fotodenah2`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `rekapsubtotal`,
0 AS `rekapdibebankankepdam`,
p.`grandtotal` AS `rekaptotal`,
0 AS `flagrablainnya`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
p.`tglrab` AS `waktuupdate`
FROM `rab_sambung_kembali` p
JOIN `kotaparepare_dataawal`.`tampung_permohonan_pelanggan_air` pp ON pp.nomorpermohonan=p.`nomorpermohonan`
LEFT JOIN `kotaparepare_dataawal`.`tampung_rekening_nonair` n ON n.`urutan`=p.`norab`
LEFT JOIN `kotaparepare_dataawal`.`master_user` u ON u.nama=p.`user`
WHERE p.`flaghapus`=0