UPDATE rekening_air_angsuran a
JOIN (
SELECT 
a.`idangsuran`,
a.`noangsuran`,
a.`jumlahangsuranpokok` AS pokok,
a.`jumlahuangmuka` AS dp,
a.`total`,
SUM(b.`total`) AS new_pokok,
SUM(b.`total`)-a.`total` AS new_dp
FROM `rekening_air_angsuran` a
JOIN `rekening_air_angsuran_detail` b ON b.`idpdam`=a.`idpdam` AND b.`idangsuran`=a.`idangsuran`
GROUP BY a.`idangsuran`
HAVING a.`jumlahangsuranpokok`<>SUM(b.`total`)
) b ON b.idangsuran=a.`idangsuran`
SET 
a.`jumlahangsuranpokok`=b.new_pokok,
a.`jumlahuangmuka`=b.new_dp
WHERE a.`idpdam`=3