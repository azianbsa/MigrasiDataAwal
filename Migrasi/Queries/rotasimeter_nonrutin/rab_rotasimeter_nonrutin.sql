DROP TEMPORARY TABLE IF EXISTS __tmp_userloket;
CREATE TEMPORARY TABLE __tmp_userloket AS
SELECT
@idpdam,
@id := @id + 1 AS iduser,
a.nama,
a.namauser
FROM (
SELECT nama,namauser,`passworduser`,alamat,aktif FROM [bacameter].`userakses`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,aktif FROM [bsbs].`userakses`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM `userloket`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM `userbshl`
) a,
(SELECT @id := 0) AS id
GROUP BY a.namauser;

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
LEFT JOIN __tmp_userloket usr ON usr.nama=rab.`user_rab_created`
WHERE l.`tglrab` IS NOT NULL