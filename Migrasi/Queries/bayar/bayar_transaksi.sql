DROP TEMPORARY TABLE IF EXISTS __tmp_userloket;
CREATE TEMPORARY TABLE __tmp_userloket AS
SELECT
@iduser := @iduser + 1 AS iduser,
nama
FROM userloket
,(SELECT @iduser := 0) AS iduser
ORDER BY nama;

DROP TEMPORARY TABLE IF EXISTS __tmp_loket;
CREATE TEMPORARY TABLE __tmp_loket AS
SELECT
@idloket := @idloket + 1 AS idloket,
kodeloket,
loket
FROM loket
,(SELECT @idloket := 0) AS idloket
ORDER BY kodeloket;

SELECT
@idpdam,
pel.id AS idpelangganair,
per.idperiode AS idperiode,
rek.nolpp AS nomortransaksi,
1 AS statustransaksi,
rek.tglbayar AS waktutransaksi,
YEAR(rek.tglbayar) AS tahuntransaksi,
usr.iduser AS iduser,
lo.idloket AS idloket,
NULL AS idkolektiftransaksi,
NULL AS idalasanbatal,
NULL AS keterangan,
NOW() AS waktuupdate
FROM
[table] rek
JOIN pelanggan pel ON pel.nosamb = rek.nosamb
JOIN __tmp_periode per ON per.periode = rek.periode
LEFT JOIN __tmp_userloket usr ON usr.nama = rek.kasir
LEFT JOIN __tmp_loket lo ON lo.kodeloket = rek.loketbayar
WHERE rek.periode = @periode
AND rek.tglbayar IS NOT NULL
AND rek.flaglunas = 1
AND rek.flagbatal = 0
AND rek.flagangsur = 0