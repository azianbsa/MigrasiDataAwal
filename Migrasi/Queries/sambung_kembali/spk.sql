SET @tgl_reg_awal='2014-11-21';
SET @tgl_reg_akhir='2025-06-20';

SELECT
@idpdam AS idpdam,
a.`idpermohonan` AS idpermohonan,
a.`no_spko` AS nomorspk,
a.`tgl_spko` AS tanggalspk,
-1 AS iduser,
1 AS flagsurvey,
0 AS flagbatal,
NULL AS idalasanbatal,
a.`tgl_spko` AS waktuupdate
FROM `sambungkembali` a
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
AND a.`no_spko`<>'-'