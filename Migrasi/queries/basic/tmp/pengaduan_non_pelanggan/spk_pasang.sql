SET @tgladu_awal='2014-01-04';
SET @tgladu_akhir='2025-07-02';

SELECT
@idpdam AS idpdam,
a.`idpermohonan` AS idpermohonan,
CONCAT(noadu,DATE_FORMAT(tgladu,'%Y%m%d'),REPLACE(jamadu,':',''),DATE_FORMAT (tgltl, '%Y%m%d')) AS nomorspk,
a.`tgltl` AS tanggalspk,
CONCAT(noadu,DATE_FORMAT(tgladu,'%Y%m%d'),REPLACE(jamadu,':',''),DATE_FORMAT (tgltl, '%Y%m%d')) AS nomorsppb,
a.`tgltl` AS tanggalsppb,
COALESCE(u.`iduser`,-1) AS iduser,
0 AS flagbatal,
NULL AS idalasanbatal,
a.`tgltl` AS waktuupdate
FROM `pengaduannonplg` a
LEFT JOIN `maros_awal`.`usermaros` u ON u.`nama`=a.`nmusertl`
WHERE a.idpermohonan=5114
-- a.`tgladu`>=@tgladu_awal
-- AND a.`tgladu`<@tgladu_akhir
-- AND a.`tgltl` IS NOT NULL