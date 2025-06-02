-- rekening_nonair_transaksi
-- new(0, "idpdam")
-- new(1, "idnonair")
-- new(2, "nomortransaksi")
-- new(3, "statustransaksi")
-- new(4, "waktutransaksi")
-- new(5, "tahuntransaksi")
-- new(6, "iduser")
-- new(7, "idloket")
-- new(8, "idkolektiftransaksi")
-- new(9, "idalasanbatal")
-- new(10, "keterangan")
-- new(11, "waktuupdate")


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