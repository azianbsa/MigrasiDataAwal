SET @tgl_reg_awal='2014-11-21';
SET @tgl_reg_akhir='2025-06-20';

SELECT
@idpdam AS idpdam,
b.`idpermohonan` AS idpermohonan,
a.`no_spko` AS nomorspk,
a.`tgl_spko` AS tanggalspk,
-1 AS iduser,
1 AS flagsurvey,
0 AS flagbatal,
NULL AS idalasanbatal,
a.`tgl_spko` AS waktuupdate
FROM `t_pelanggan_reg` a
JOIN `sambunganbaru` b ON b.`no_reg`=a.`no_reg`
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
AND a.`no_spko`<>'-'
AND a.no_reg IN (
'0005/PMP/HL/VI/2025'
)