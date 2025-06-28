SET @tgl_reg_awal='2017-12-18';
SET @tgl_reg_akhir='2025-01-10';

SELECT
@idpdam AS `idpdam`,
g.`idpermohonan` AS `idpermohonan`,
'Golongan Air Baru' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
COALESCE(go.`idgolongan`,30) AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_reg_pg` a
JOIN `rubahgol` g ON g.`no_reg`=a.`no_reg`
LEFT JOIN `t_goltarif` t ON t.`KDGT`=a.`kd_gol_baru`
LEFT JOIN `maros_awal`.`golonganmaros` go ON go.`kodegolongan`=t.`GOLTARIF`
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
UNION ALL
SELECT
@idpdam AS `idpdam`,
g.`idpermohonan` AS `idpermohonan`,
'Golongan Air Lama' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
COALESCE(go.`idgolongan`,30) AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_reg_pg` a
JOIN `rubahgol` g ON g.`no_reg`=a.`no_reg`
LEFT JOIN `t_goltarif` t ON t.`KDGT`=a.`kd_gol_lama`
LEFT JOIN `maros_awal`.`golonganmaros` go ON go.`kodegolongan`=t.`GOLTARIF`
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir