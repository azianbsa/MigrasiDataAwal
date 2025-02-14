DROP TEMPORARY TABLE IF EXISTS __tmp_userbshl;
CREATE TEMPORARY TABLE __tmp_userbshl AS
SELECT
@iduser := @iduser + 1 AS iduser,
nama
FROM `userbshl`
,(SELECT @iduser := 0) AS iduser
ORDER BY nama;

SELECT
@idpdam AS `idpdam`,
r.`idpermohonan` AS `idpermohonan`,
@jenisnonair AS `idjenisnonair`,
na.`id` AS `idnonair`,
rab.`norab` AS `nomorrab`,
l.`tglrab` AS `tanggalrab`,
l.`nomorbppi` AS `nomorbppi`,
l.`tanggalbppi` AS `tanggalbppi`,
usr.iduser AS `iduser`,
l.`validdate` AS `tanggalkadaluarsa`,
l.`namapaketrab` AS  `persilnamapaket`,
IFNULL(l.`dialihkankevendor`,0) AS `persilflagdialihkankevendor`,
IFNULL(l.`biayadibebankankepdam`,0) AS `persilflagbiayadibebankankepdam`,
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
l.`grandtotal` AS `rekaptotal`,
0 AS `flagrablainnya`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
l.`tglrab` AS `waktuupdate`
FROM
`rotasimeter_nonrutin` rab
JOIN __tmp_rotasimeter r ON r.nosamb=rab.`nosamb` AND r.periode=rab.`periode`
JOIN rab_lainnya l ON l.`norab`=rab.`norab`
LEFT JOIN nonair na ON na.`urutan`=rab.`norab`
LEFT JOIN __tmp_userbshl usr ON usr.nama=rab.`user_rab_created`
WHERE l.`tglrab` IS NOT NULL