SET @tgl_reg_awal='2017-03-31';
SET @tgl_reg_akhir='2025-06-26';

SELECT
@idpdam AS idpdam,
g.`idpermohonan` AS idpermohonan,
g.`nomorpermohonan` AS nomorspk,
a.`tgl_cm` AS tanggalspk,
g.`nomorpermohonan` AS nomorsppb,
a.`tgl_cm` AS tanggalsppb,
-1 AS iduser,
0 AS flagbatal,
NULL AS idalasanbatal,
a.`tgl_cm` AS waktuupdate
FROM `t_reg_cm` a
JOIN `tutuptotaltunggakan` g ON g.`no_cm`=a.`no_cm`
WHERE a.`tgl_cm`>=@tgl_reg_awal
AND a.`tgl_cm`<@tgl_reg_akhir