﻿DROP TEMPORARY TABLE IF EXISTS __tmp_userbshl;
CREATE TEMPORARY TABLE __tmp_userbshl AS
SELECT
@iduser := @iduser + 1 AS iduser,
nama
FROM `userbshl`
,(SELECT @iduser := 0) AS iduser
ORDER BY nama;

SELECT
@idpdam AS `idpdam`,
per.`idpermohonan` AS `idpermohonan`,
@jenisnonair AS `idjenisnonair`,
na.`id` AS `idnonair`,
rab.`norab` AS `nomorrab`,
rab.`tglrab` AS `tanggalrab`,
'-' AS `nomorbppi`,
rab.`tglrab` AS `tanggalbppi`,
usr.iduser AS `iduser`,
rab.`validdate` AS `tanggalkadaluarsa`,
`namapaketrab` AS  `persilnamapaket`,
IFNULL(rab.`dialihkankevendor`,0) AS `persilflagdialihkankevendor`,
IFNULL(rab.`biayadibebankankepdam`,0) AS `persilflagbiayadibebankankepdam`,
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
rab.`grandtotal` AS `rekaptotal`,
0 AS `flagrablainnya`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
rab.`tglrab` AS `waktuupdate`
FROM
`rab_sambung_kembali` rab
JOIN __tmp_sambung_kembali per ON per.`nomor`=rab.`nomorpermohonan`
LEFT JOIN nonair na ON na.`urutan`=rab.`norab`
LEFT JOIN __tmp_userbshl usr ON usr.nama=rab.`user`
WHERE rab.`flaghapus`=0 and rab.tglrab is not null