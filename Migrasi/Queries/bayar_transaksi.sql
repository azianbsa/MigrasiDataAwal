SELECT
 @idpdam,
 pel.id AS idpelangganair,
 per.idperiode,
 byr.nolpp AS nomortransaksi,
 1 AS statustransaksi,
 byr.tglbayar AS waktutransaksi,
 LEFT(byr.periode, 4) AS tahuntransaksi,
 usr.iduser,
 lo.idloket,
 NULL AS idkolektiftransaksi,
 NULL AS idalasanbatal,
 NULL AS keterangan,
 byr.waktu AS waktuupdate
FROM
 [table] byr
 LEFT JOIN pelanggan pel ON pel.nosamb = byr.nosamb
 LEFT JOIN (
 SELECT
 @idperiode := @idperiode + 1 AS idperiode,
 periode
 FROM periode
 ,(SELECT @idperiode := 0) AS idperiode
 ORDER BY periode 
 ) per ON per.periode = byr.periode
 LEFT JOIN (
 SELECT
 @iduser := @iduser + 1 AS iduser,
 nama
 FROM [dbloket].userloket
 ,(SELECT @iduser := 0) AS iduser
 ORDER BY nama
 ) usr ON LOWER(usr.nama) = LOWER(byr.kasir)
 LEFT JOIN (
 SELECT
 @idloket := @idloket + 1 AS idloket,
 loket
 FROM [dbloket].loket
 ,(SELECT @idloket := 0) AS idloket
 ORDER BY kodeloket
 ) lo ON LOWER(lo.loket) = LOWER(byr.namaloket)
 WHERE byr.flaglunas = 1 AND byr.flagbatal = 0;