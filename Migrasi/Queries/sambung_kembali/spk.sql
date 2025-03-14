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

DROP TEMPORARY TABLE IF EXISTS __tmp_sambung_kembali;
CREATE TEMPORARY TABLE __tmp_sambung_kembali AS
SELECT
@id := @id+1 AS ID,
p.nomor
FROM permohonan_sambung_kembali P
,(SELECT @id := @lastid) AS id
WHERE p.flaghapus=0;

SELECT
@idpdam AS `idpdam`,
p.id AS `idpermohonan`,
pp.`nomorspkopname` AS `nomorspk`,
pp.`tglspko` AS `tanggalspk`,
usr.iduser AS `iduser`,
IF(pp.`disetujui`=1,1,2) AS `flagsurvey`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
pp.`tglselesaiopname` AS `waktuupdate`
FROM __tmp_sambung_kembali p
JOIN `spk_opname_sambung_kembali` pp ON pp.nomorpermohonan=p.nomor
LEFT JOIN __tmp_userloket usr ON usr.nama=pp.user
WHERE pp.flaghapus=0 AND pp.tglspko IS NOT NULL