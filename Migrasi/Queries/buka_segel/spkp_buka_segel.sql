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
per.`idpermohonan` AS `idpermohonan`,
spk.`nomorspk` AS `nomorspk`,
spk.`tanggalspk` AS `tanggalspk`,
null AS `nomorsppb`,
null AS `tanggalsppb`,
usr.iduser AS `iduser`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
spk.`tanggalspk` AS `waktuupdate`
FROM
`spk_bukasegel` spk
JOIN __tmp_buka_segel per ON per.nomor = spk.nomorpermohonan
LEFT JOIN __tmp_userloket usr ON usr.nama = spk.user
WHERE spk.flaghapus=0;