DROP TEMPORARY TABLE IF EXISTS __tmp_userloket;
CREATE TEMPORARY TABLE __tmp_userloket AS
SELECT
@iduser := @iduser + 1 AS iduser,
nama
FROM userloket
,(SELECT @iduser := 0) AS iduser
ORDER BY nama;

SELECT
@idpdam AS `idpdam`,
per.`idpermohonan` AS `idpermohonan`,
@jenisnonair AS `idjenisnonair`,
NULL AS `idnonair`,
rab.`no_rab` AS `nomorrab`,
r.`tglrab` AS `tanggalrab`,
r.`nomorbppi` AS `nomorbppi`,
r.`tanggalbppi` AS `tanggalbppi`,
usr.iduser AS `iduser`,
r.`validdate` AS `tanggalkadaluarsa`,
`namapaketrab` AS  `persilnamapaket`,
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
NULL AS `fotodenah1`,
NULL AS `fotodenah2`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `rekapsubtotal`,
0 AS `rekapdibebankankepdam`,
r.`grandtotal` AS `rekaptotal`,
0 AS `flagrablainnya`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
r.`tglrab` AS `waktuupdate`
FROM
`permohonan_rubah_rayon` rab
JOIN __tmp_permohonan_rubah_rayon per ON per.`nomor`=rab.`nomor`
LEFT JOIN rab_lainnya r ON r.norab=rab.`no_rab`
LEFT JOIN __tmp_userloket usr ON usr.nama=rab.`user_input`
WHERE rab.`flaghapus`=0 AND rab.`no_rab` IS NOT NULL