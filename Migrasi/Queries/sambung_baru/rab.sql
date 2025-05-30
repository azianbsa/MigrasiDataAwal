-- permohonan_non_pelanggan_rab
-- new(0, "idpdam")
-- new(1, "idpermohonan")
-- new(2, "idjenisnonair")
-- new(3, "idnonair")
-- new(4, "nomorrab")
-- new(5, "tanggalrab")
-- new(6, "nomorbppi")
-- new(7, "tanggalbppi")
-- new(8, "iduserbppi")
-- new(9, "iduser")
-- new(10, "tanggalkadaluarsa")
-- new(11, "persilnamapaket")
-- new(12, "persilflagdialihkankevendor")
-- new(13, "persilflagbiayadibebankankepdam")
-- new(14, "persilsubtotal")
-- new(15, "persildibebankankepdam")
-- new(16, "persiltotal")
-- new(17, "distribusinamapaket")
-- new(18, "distribusiflagdialihkankevendor")
-- new(19, "distribusiflagbiayadibebankankepdam")
-- new(20, "distribusisubtotal")
-- new(21, "distribusidibebankankepdam")
-- new(22, "distribusitotal")
-- new(23, "rekapsubtotal")
-- new(24, "rekapdibebankankepdam")
-- new(25, "rekaptotal")
-- new(26, "flagrablainnya")
-- new(27, "flagbatal")
-- new(28, "idalasanbatal")
-- new(29, "waktuupdate")

SET @idtipepermohonan=(SELECT idtipepermohonan FROM [dataawal].`master_attribute_tipe_permohonan` WHERE idpdam=@idpdam AND `kodetipepermohonan`='SAMBUNGAN_BARU_AIR');

SELECT
@idpdam AS `idpdam`,
p.idpermohonan AS `idpermohonan`,
-1 AS `idjenisnonair`,
n.idnonair AS `idnonair`,
r.`norab` AS `nomorrab`,
r.`tglrab` AS `tanggalrab`,
r.`nomorbppi` AS `nomorbppi`,
r.`tanggalbppi` AS `tanggalbppi`,
IF(r.`nomorbppi` IS NULL,NULL,u.iduser) AS `iduserbppi`,
u.iduser AS `iduser`,
r.`validdate` AS `tanggalkadaluarsa`,
r.`namapaketrab` AS `persilnamapaket`,
IFNULL(r.`dialihkankevendor`,0) AS `persilflagdialihkankevendor`,
IFNULL(r.`biayadibebankankepdam`,0) AS `persilflagbiayadibebankankepdam`,
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
r.`grandtotal` AS `rekaptotal`,
0 AS `flagrablainnya`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
r.`tglrab` AS `waktuupdate`
FROM rab r
JOIN [dataawal].`tampung_permohonan_non_pelanggan` p ON p.nomorpermohonan=r.`nomorreg` AND p.idtipepermohonan=@idtipepermohonan AND p.idpdam=@idpdam
LEFT JOIN [dataawal].`master_user` u ON u.nama=r.`user` AND u.idpdam=@idpdam
LEFT JOIN [dataawal].`tampung_rekening_nonair` n ON n.nomornonair=r.nomorreg AND n.idpdam=@idpdam
WHERE r.flaghapus=0