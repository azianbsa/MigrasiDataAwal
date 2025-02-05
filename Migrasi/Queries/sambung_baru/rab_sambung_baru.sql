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
p.`idpermohonan` AS `idpermohonan`,
-1 AS `idjenisnonair`,
NULL AS `idnonair`,
rab.`norab` AS `nomorrab`,
rab.`tglrab` AS `tanggalrab`,
rab.`nomorbppi` AS `nomorbppi`,
rab.`tanggalbppi` AS `tanggalbppi`,
usr.iduser AS `iduser`,
rab.`validdate` AS `tanggalkadaluarsa`,
rab.`namapaketrab` AS  `persilnamapaket`,
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
COALESCE(rab.`tglrab`,NOW()) AS `waktuupdate`
FROM
`rab` rab
JOIN __tmp_sambung_baru p ON p.`nomorreg`=rab.`nomorreg`
LEFT JOIN __tmp_userbshl usr ON usr.nama=rab.`user`
WHERE rab.`flaghapus`=0 AND rab.`norab` IS NOT NULL