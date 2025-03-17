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
spkp.`nomorspkp` AS `nomorspk`,
spkp.`tanggalspkp` AS `tanggalspk`,
spkp.`nomorsppb` AS `nomorsppb`,
spkp.`tanggalspkp` AS `tanggalsppb`,
usr.iduser AS `iduser`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
COALESCE(spkp.`tanggalspkp`,spkp.`tglpasang`) AS `waktuupdate`
FROM __tmp_pendaftaran p
JOIN `rab` spkp ON `spkp`.`nomorreg`=p.`nomorreg`
LEFT JOIN __tmp_userloket usr ON usr.nama=spkp.`user`
WHERE `spkp`.`flaghapus`=0 AND spkp.`nomorspkp` IS NOT NULL