SET @tgl_reg_awal='2014-11-21';
SET @tgl_reg_akhir='2025-06-20';

SELECT
@idpdam AS idpdam,
a.`idpermohonan` AS idpermohonan,
a.`no_spk` AS nomorspk,
a.`tgl_spk` AS tanggalspk,
a.`no_spk` AS nomorsppb,
a.`tgl_spk` AS tanggalsppb,
-1 AS iduser,
0 AS flagbatal,
NULL AS idalasanbatal,
a.`tgl_spk` AS waktuupdate
FROM `sambungkembali` a
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
AND a.`no_spk`<>'-'