SET @tgl_reg_awal='2017-01-01';
SET @tgl_reg_akhir='2025-07-01';

SELECT
@idpdam AS idpdam,
b.`idpermohonan` AS idpermohonan,
a.`no_reg` AS nomorba,
a.`tgl_bn` AS tanggalba,
-1 AS iduser,
NULL AS persilnamapaket,
0 AS persilflagdialihkankevendor,
0 AS persilflagbiayadibebankankepdam,
NULL AS distribusinamapaket,
0 AS distribusiflagdialihkankevendor,
0 AS distribusiflagbiayadibebankankepdam,
0 AS flagbatal,
NULL AS idalasanbatal,
1 AS flag_dari_verifikasi,
NULL AS statusberitaacara,
a.`tgl_bn` AS waktuupdate
FROM `t_reg_bn` a
JOIN `baliknama` b ON b.`no_reg`=a.`no_reg`
WHERE a.tgl_reg>=@tgl_reg_awal
AND a.tgl_reg<@tgl_reg_akhir
AND a.`tgl_bn` IS NOT NULL