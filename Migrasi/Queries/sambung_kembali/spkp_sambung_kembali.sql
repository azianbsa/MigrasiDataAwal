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
spkp.`nomorspkp` AS `nomorspk`,
spkp.`tglpasang` AS `tanggalspk`,
spkp.`nomorsppb` AS `nomorsppb`,
spkp.`tglrab` AS `tanggalsppb`,
usr.iduser AS `iduser`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
spkp.`tglpasang` AS `waktuupdate`
FROM
`rab_sambung_kembali` spkp
JOIN __tmp_sambung_kembali per ON per.`nomor`=spkp.`nomorpermohonan`
LEFT JOIN __tmp_userbshl usr ON usr.nama=spkp.`user`
WHERE spkp.`nomorspkp` IS NOT NULL AND `spkp`.`flaghapus`=0