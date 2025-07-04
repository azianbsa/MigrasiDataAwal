SET @tgladu_awal='2014-01-04';
SET @tgladu_akhir='2025-07-02';

SELECT
@idpdam AS idpdam,
a.`idpermohonan` AS idpermohonan,
b.`parameter` AS parameter,
b.`tipedata` AS tipedata,
CASE 
	WHEN b.parameter='Keterangan' THEN a.uraiantl
	WHEN b.parameter='Keterangan Lapangan' THEN a.uraiantl
END AS valuestring,
NULL AS valuedecimal,
NULL AS valueinteger,
NULL AS valuedate,
NULL AS valuebool,
a.`tglenttl` AS waktuupdate
FROM `pengaduanplg` a
JOIN `maros_awal`.`tipepermohonan` t ON t.`kodejenisnonair`=a.`jns`
JOIN `maros_awal`.`tipepermohonandetailba` b ON b.`idtipepermohonan`=t.`idtipepermohonan`
LEFT JOIN `maros_awal`.`usermaros` u ON u.`nama`=a.`nmusertl`
WHERE 
-- a.`tgladu`>=@tgladu_awal
-- AND a.`tgladu`<@tgladu_akhir
-- AND 
a.`tglenttl` IS NOT NULL