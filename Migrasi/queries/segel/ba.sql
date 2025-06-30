SET @tgl_reg_awal='2002-01-01';
SET @tgl_reg_akhir='2019-01-01';

SELECT
@idpdam AS idpdam,
g.`idpermohonan` AS idpermohonan,
g.`nomorpermohonan` AS nomorba,
a.`tgl_sm` AS tanggalba,
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
a.`tgl_sm` AS waktuupdate
FROM `t_reg_sm` a
JOIN `maros_awal`.`pelangganmaros` p ON p.`nosamb`=a.`nosamb`
JOIN `segeltunggakan` g ON g.`no_sm`=a.`no_sm`
WHERE a.`tgl_sm`>=@tgl_reg_awal
AND a.`tgl_sm`<@tgl_reg_akhir