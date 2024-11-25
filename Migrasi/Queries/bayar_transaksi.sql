DROP TEMPORARY TABLE IF EXISTS temp_dataawal_periode;
CREATE TEMPORARY TABLE temp_dataawal_periode (
    idperiode INT,
    periode VARCHAR(10),
    INDEX idx_temp_dataawal_periode_periode (periode)
);
INSERT INTO temp_dataawal_periode
SELECT
@idperiode:=@idperiode+1 AS idperiode,
periode
FROM periode
,(SELECT @idperiode:=0) AS idperiode
ORDER BY periode;

DROP TEMPORARY TABLE IF EXISTS temp_dataawal_userloket;
CREATE TEMPORARY TABLE temp_dataawal_userloket (
    iduser INT,
    nama VARCHAR(30),
    INDEX idx_temp_dataawal_userloket_nama (nama)
);
INSERT INTO temp_dataawal_userloket
SELECT
@iduser := @iduser + 1 AS iduser,
nama
FROM [dbloket].userloket
,(SELECT @iduser := 0) AS iduser
ORDER BY nama;

DROP TEMPORARY TABLE IF EXISTS temp_dataawal_loket;
CREATE TEMPORARY TABLE temp_dataawal_loket (
    idloket INT,
    loket VARCHAR(50),
    INDEX idx_temp_dataawal_loket_loket (loket)
);
INSERT INTO temp_dataawal_loket
SELECT
@idloket := @idloket + 1 AS idloket,
loket
FROM [dbloket].loket
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
 rek.waktu AS waktuupdate
FROM
 [table] rek
 LEFT JOIN pelanggan pel ON pel.nosamb = rek.nosamb
 LEFT JOIN temp_dataawal_periode per ON per.periode = rek.periode
 LEFT JOIN temp_dataawal_userloket usr ON usr.nama = rek.kasir
 LEFT JOIN temp_dataawal_loket lo ON lo.loket = rek.namaloket
 WHERE rek.kode = CONCAT(rek.periode, '.', rek.nosamb) AND rek.flagangsur = 0 AND rek.flaglunas = 1 AND rek.flagbatal = 0;