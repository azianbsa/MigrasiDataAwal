SELECT
@idpdam AS `idpdam`,
rg.`id` AS `idpermohonan`,
per.`nospk_pengecekan` AS `nomorspk`,
per.`tglspk_pengecekan` AS `tanggalspk`,
rg.`iduser` AS `iduser`,
1 AS `flagsurvey`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
per.`tglspk_pengecekan` AS `waktuupdate`
FROM `permohonan_rubah_gol` per
JOIN __tmp_permohonan_rubah_gol rg ON rg.nomor=per.nomor
WHERE per.flaghapus = 0 AND per.`flag_spk_pengecekan`=1