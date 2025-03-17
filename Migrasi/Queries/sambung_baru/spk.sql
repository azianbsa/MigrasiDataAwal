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

DROP TABLE IF EXISTS __tmp_pendaftaran;
CREATE TABLE __tmp_pendaftaran AS
SELECT
@id:=@id+1 AS id,
nomorreg
FROM `pendaftaran`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
spk.`nomorspkopname` AS `nomorspk`,
spk.`tglspko` AS `tanggalspk`,
usr.iduser AS `iduser`,
IF(spk.`disetujui`=1,1,2) AS `flagsurvey`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
COALESCE(spk.`tglspko`,NOW()) AS `waktuupdate`
FROM
__tmp_pendaftaran p
JOIN `spk_opname_sambung_baru` spk ON spk.`nomorreg`=p.`nomorreg`
LEFT JOIN __tmp_userloket usr ON usr.nama=spk.user
WHERE spk.flaghapus=0 AND spk.`tglspko` IS NOT NULL