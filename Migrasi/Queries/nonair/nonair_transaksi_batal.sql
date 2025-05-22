SELECT
@idpdam,
t.idnonair AS idnonair,
n.nolpp AS nomortransaksi,
0 AS statustransaksi,
n.waktubayar AS waktutransaksi,
YEAR(n.waktubayar) AS tahuntransaksi,
u.iduser AS iduser,
l.idloket AS idloket,
NULL AS idkolektiftransaksi,
NULL AS idalasanbatal,
n.keterangan AS keterangan,
n.waktuupdate AS waktuupdate
FROM nonair n
JOIN [dataawal].`tampung_rekening_nonair` t ON t.urutan=n.urutan AND t.idpdam=@idpdam
LEFT JOIN [dataawal].`master_user` u ON u.nama=n.kasir AND u.idpdam=@idpdam
LEFT JOIN [dataawal].`master_attribute_loket` l ON l.kodeloket=n.loketbayar AND l.idpdam=@idpdam
WHERE n.flagbatal=1