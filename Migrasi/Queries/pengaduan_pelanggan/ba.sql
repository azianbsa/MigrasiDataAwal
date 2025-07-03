SET @tgladu_awal='2014-01-04';
SET @tgladu_akhir='2025-07-02';

SELECT
@idpdam AS idpdam,
a.`idpermohonan` idpermohonan,
a.`no_bukti` AS nomorba,
a.`tglenttl` AS tanggalba,
COALESCE(u.`iduser`,-1) iduser,
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
a.`tglenttl` AS waktuupdate
FROM `pengaduanplg` a
LEFT JOIN `maros_awal`.`usermaros` u ON u.`nama`=a.`nmusertl`
WHERE a.`tgladu`>=@tgladu_awal
AND a.`tgladu`<@tgladu_akhir
AND a.`tglenttl` IS NOT NULL