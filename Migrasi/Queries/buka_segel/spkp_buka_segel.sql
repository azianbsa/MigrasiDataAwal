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
LEFT JOIN __tmp_userbshl usr ON usr.nama = spk.user
WHERE spk.flaghapus=0;