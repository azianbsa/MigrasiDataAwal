SET @tgl_reg_awal='2013-01-01';
SET @tgl_reg_akhir='2025-06-01';

SELECT
@idpdam AS idpdam,
b.`idpermohonan` AS idpermohonan,
-1 AS idjenisnonair,
c.`idnonair` AS idnonair,
a.`no_rab` AS nomorrab,
a.`tgl_rab` AS tanggalrab,
IF(a.`no_bppi`<>'-',a.`no_bppi`,NULL) AS nomorbppi,
IF(a.`no_bppi`<>'-',a.`tgl_bppi`,NULL) AS tanggalbppi,
-1 AS iduserbppi,
-1 AS iduser,
NULL AS tanggalkadaluarsa,
-- persilnamapaket,
-- persilflagdialihkankevendor,
-- persilflagbiayadibebankankepdam,
-- persilsubtotal,
-- persildibebankankepdam,
-- persiltotal,
-- distribusinamapaket,
-- distribusiflagdialihkankevendor,
-- distribusiflagbiayadibebankankepdam,
-- distribusisubtotal,
-- distribusidibebankankepdam,
-- distribusitotal,
-- rekapsubtotal,
-- rekapdibebankankepdam,
-- rekaptotal,
0 AS flagrablainnya,
0 AS flagbatal,
NULL AS idalasanbatal,
a.`tgl_rab` AS waktuupdate
FROM `t_pelanggan_reg` a
JOIN `pelanggan_reg` b ON b.`no_reg`=a.`no_reg`
LEFT JOIN `migrasi_nonair` c ON c.`no_reg`=a.`no_reg` AND c.`jns`='B02'
WHERE a.`no_rab`<>'-'
AND a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<=@tgl_reg_akhir