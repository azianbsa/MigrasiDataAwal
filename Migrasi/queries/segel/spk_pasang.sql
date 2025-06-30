SET @tgl_reg_awal='2002-01-01';
SET @tgl_reg_akhir='2019-01-01';

SELECT
@idpdam AS idpdam,
g.`idpermohonan` AS idpermohonan,
g.`nomorpermohonan` AS nomorspk,
a.`tgl_sm` AS tanggalspk,
g.`nomorpermohonan` AS nomorsppb,
a.`tgl_sm` AS tanggalsppb,
-1 AS iduser,
0 AS flagbatal,
NULL AS idalasanbatal,
a.`tgl_sm` AS waktuupdate
FROM `t_reg_sm` a
JOIN `maros_awal`.`pelangganmaros` p ON p.`nosamb`=a.`nosamb`
JOIN `segeltunggakan` g ON g.`no_sm`=a.`no_sm`
WHERE a.`tgl_sm`>=@tgl_reg_awal
AND a.`tgl_sm`<@tgl_reg_akhir