SELECT
@idpdam AS idpdam,
pel.`idpelangganair` AS idpelangganair,
per.`idperiode` AS idperiode,
rek.`NO_BAYAR` AS nomortransaksi,
1 AS statustransaksi,
rek.`TANGGALBAYAR` AS waktutransaksi,
YEAR(rek.`TANGGALBAYAR`) AS tahuntransaksi,
COALESCE(us.`iduser`,-1) AS iduser,
l.`idloket` AS idloket,
NULL AS idkolektiftransaksi,
0 AS idalasanbatal,
NULL AS keterangan,
rek.`TANGGALBAYAR` AS waktuupdate
FROM `t_penjualan` rek
JOIN `maros_awal`.`pelangganmaros` pel ON pel.nosamb=rek.nosamb
JOIN `maros_awal`.`periodemaros` per ON per.kodeperiode=CONCAT(rek.`PERIODE2`,LPAD(rek.`PERIODE1`,2,'0'))
LEFT JOIN `maros_awal`.`t_user` u ON u.`NO_ID`=rek.`USER_BAYAR`
LEFT JOIN `maros_awal`.`usermaros` us ON `us`.`nama`=u.`NAMA_USER`
LEFT JOIN `maros_awal`.`loketmaros` l ON l.`kodeloket`=rek.`LOKET_BAYAR`
LEFT JOIN `t_penjualan_hps` hps ON hps.`nosamb`=rek.`nosamb` AND CONCAT(hps.`PERIODE2`,LPAD(hps.`PERIODE1`,2,'0'))=CONCAT(rek.`PERIODE2`,LPAD(rek.`PERIODE1`,2,'0'))
WHERE rek.`TANGGALBAYAR` IS NOT NULL 
AND rek.`TANGGALBAYAR`>='2025-06-26' 
AND rek.`TANGGALBAYAR`<'2025-06-28'
AND (hps.`NO_BUKTI` IS NULL OR rek.`TANGGALBAYAR`>hps.`TANGGALBAYAR`)