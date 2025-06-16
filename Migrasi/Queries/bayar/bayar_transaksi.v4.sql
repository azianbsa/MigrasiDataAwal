SELECT
@idpdam,
p.id AS idpelangganair,
pr.idperiode AS idperiode,
b.nolpp AS nomortransaksi,
1 AS statustransaksi,
b.tglbayar AS waktutransaksi,
YEAR(b.tglbayar) AS tahuntransaksi,
u.iduser AS iduser,
l.idloket AS idloket,
NULL AS idkolektiftransaksi,
0 AS idalasanbatal,
NULL AS keterangan,
NOW() AS waktuupdate
FROM bayar b
JOIN pelanggan p ON p.nosamb=b.nosamb
JOIN [dataawal].`master_periode` pr ON pr.`kodeperiode`=b.periode AND pr.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_user` u ON u.nama=b.kasir AND u.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_loket` l ON l.kodeloket=b.loketbayar AND l.`idpdam`=@idpdam
WHERE b.periode BETWEEN 202502 AND 202504
AND b.flagangsur=0