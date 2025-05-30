--permohonan_pelanggan_air_rab
--new(0, "idpdam")
--new(1, "idpermohonan")
--new(2, "idjenisnonair")
--new(3, "idnonair")
--new(4, "nomorrab")
--new(5, "tanggalrab")
--new(6, "nomorbppi")
--new(7, "tanggalbppi")
--new(8, "iduserbppi")
--new(9, "iduser")
--new(10, "tanggalkadaluarsa")
--new(11, "persilnamapaket")
--new(12, "persilflagdialihkankevendor")
--new(13, "persilflagbiayadibebankankepdam")
--new(14, "persilsubtotal")
--new(15, "persildibebankankepdam")
--new(16, "persiltotal")
--new(17, "distribusinamapaket")
--new(18, "distribusiflagdialihkankevendor")
--new(19, "distribusiflagbiayadibebankankepdam")
--new(20, "distribusisubtotal")
--new(21, "distribusidibebankankepdam")
--new(22, "distribusitotal")
--new(23, "rekapsubtotal")
--new(24, "rekapdibebankankepdam")
--new(25, "rekaptotal")
--new(26, "flagrablainnya")
--new(27, "flagbatal")
--new(28, "idalasanbatal")
--new(29, "waktuupdate")

SET @idjenisnonair=(SELECT `idjenisnonair` FROM `kotaparepare_dataawal`.`master_attribute_jenis_nonair` WHERE idpdam=@idpdam AND `namajenisnonair`='SAMB.KEMBALI');

SELECT
@idpdam AS `idpdam`,
pp.`idpermohonan` AS `idpermohonan`,
@idjenisnonair AS `idjenisnonair`,
n.`idnonair` AS `idnonair`,
p.`norab` AS `nomorrab`,
p.`tglrab` AS `tanggalrab`,
REPLACE(p.`norab`,'/RAB SAMB.KEMBALI/','/BPPI SAMB.KEMBALI/') AS `nomorbppi`,
p.`tglrab` AS `tanggalbppi`,
NULL AS `iduserbppi`,
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