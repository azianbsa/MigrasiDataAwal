SET @tgl_reg_awal='2017-12-18';
SET @tgl_reg_akhir='2025-01-10';

SELECT
@idpdam AS idpdam,
g.`idpermohonan` AS idpermohonan,
a.`no_reg` AS nomorba,
a.`tgl_pg` AS tanggalba,
-101 AS iduser,
NULL AS persilnamapaket,
0 AS persilflagdialihkankevendor,
0 AS persilflagbiayadibebankankepdam,
NULL AS distribusinamapaket,
0 AS distribusiflagdialihkankevendor,
0 AS distribusiflagbiayadibebankankepdam,
0 AS flagbatal,
NULL AS idalasanbatal,
1 AS flag_dari_verifikasi,
'Berhasil Dikerjakan' AS statusberitaacara,
a.`tgl_pg` AS waktuupdate
FROM `t_reg_pg` a
JOIN `maros_awal`.`pelangganmaros` p ON p.`nosamb`=a.`nosamb`
JOIN `rubahgol` g ON g.`no_reg`=a.`no_reg`
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
AND a.`tgl_pg` IS NOT NULL