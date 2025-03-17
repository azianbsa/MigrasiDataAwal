DROP TABLE IF EXISTS __tmp_pendaftaran;
CREATE TABLE __tmp_pendaftaran AS
SELECT
@id:=@id+1 AS id,
nomorreg
FROM `pendaftaran`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

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
p.`id` AS `idpermohonan`,
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
rab.`tglrab` AS `waktuupdate`
FROM
__tmp_pendaftaran p
JOIN `rab` rab ON rab.`nomorreg`=p.`nomorreg`
LEFT JOIN __tmp_userloket usr ON usr.nama=rab.`user`
WHERE rab.`flaghapus`=0 AND rab.`tglrab` IS NOT NULL