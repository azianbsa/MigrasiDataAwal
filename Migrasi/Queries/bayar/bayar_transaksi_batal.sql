/* rekening_air_transaksi
 * new(0, "idpdam")
 * new(1, "idpelangganair")
 * new(2, "idperiode")
 * new(3, "nomortransaksi")
 * new(4, "statustransaksi")
 * new(5, "waktutransaksi")
 * new(6, "tahuntransaksi")
 * new(7, "iduser")
 * new(8, "idloket")
 * new(9, "idkolektiftransaksi")
 * new(10, "idalasanbatal")
 * new(11, "keterangan")
 * new(12, "waktuupdate")
 */

SELECT
@idpdam,
p.id AS idpelangganair,
pr.idperiode AS idperiode,
b.nolpp AS nomortransaksi,
0 AS statustransaksi,
b.tglbayar AS waktutransaksi,
YEAR(b.tglbayar) AS tahuntransaksi,
u.iduser AS iduser,
l.idloket AS idloket,
NULL AS idkolektiftransaksi,
0 AS idalasanbatal,
NULL AS keterangan,
b.tglbatal AS waktuupdate
FROM piutang b
JOIN pelanggan p ON p.nosamb=b.nosamb
JOIN [dataawal].`master_periode` pr ON pr.`kodeperiode`=b.periode AND pr.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_user` u ON u.nama=b.kasir AND u.`idpdam`=@idpdam
LEFT JOIN [dataawal].`master_attribute_loket` l ON l.kodeloket=b.loketbayar AND l.`idpdam`=@idpdam
WHERE b.periode BETWEEN 202502 AND 202504
AND b.`flagangsur`=0
AND b.`flagbatal`=1