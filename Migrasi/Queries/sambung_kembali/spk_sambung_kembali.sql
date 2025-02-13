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
`tglselesaiopname` AS `waktuupdate`
FROM
`spk_opname_sambung_kembali` spk
JOIN __tmp_sambung_kembali per ON per.nomor = spk.nomorpermohonan
LEFT JOIN __tmp_userbshl usr ON usr.nama = spk.user
WHERE spk.flaghapus=0 and spk.tglspko is not null