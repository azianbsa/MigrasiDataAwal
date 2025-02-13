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
spk.`nomorspkopname` AS `nomorspk`,
spk.`tglspko` AS `tanggalspk`,
usr.iduser AS `iduser`,
IF(spk.`disetujui`=1,1,2) AS `flagsurvey`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
coalesce(spk.`tglselesaiopname`,now()) AS `waktuupdate`
FROM
`spk_opname_sambung_baru` spk
JOIN __tmp_sambung_baru per ON per.nomorreg = spk.`nomorreg`
LEFT JOIN __tmp_userbshl usr ON usr.nama = spk.user
WHERE spk.flaghapus=0 AND spk.`nomorspkopname` IS NOT NULL