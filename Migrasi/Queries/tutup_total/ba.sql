SET @tgl_reg_awal='2025-06-28';
SET @tgl_reg_akhir='2025-07-01';

SELECT
@idpdam AS idpdam,
g.`idpermohonan` AS idpermohonan,
g.`nomorpermohonan` AS nomorba,
a.`tgl_cm` AS tanggalba,
-1 AS iduser,
NULL AS persilnamapaket,
0 AS persilflagdialihkankevendor,
0 AS persilflagbiayadibebankankepdam,
NULL AS distribusinamapaket,
0 AS distribusiflagdialihkankevendor,
0 AS distribusiflagbiayadibebankankepdam,
0 AS flagbatal,
NULL AS idalasanbatal,
NULL AS flag_dari_verifikasi,
'Berhasil Dikerjakan' AS statusberitaacara,
a.`tgl_cm` AS waktuupdate
FROM `t_reg_cm` a
JOIN `tutuptotaltunggakan` g ON g.`no_cm`=a.`no_cm`
WHERE a.`tgl_cm`>=@tgl_reg_awal
AND a.`tgl_cm`<@tgl_reg_akhir