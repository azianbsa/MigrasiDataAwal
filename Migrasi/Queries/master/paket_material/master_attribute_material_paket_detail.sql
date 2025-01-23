DROP TEMPORARY TABLE IF EXISTS temp_dataawal_barang;
CREATE TEMPORARY TABLE temp_dataawal_barang AS
SELECT
 @id := @id+1 AS idmaterial,
 kodebarang AS kodematerial
FROM
 barang,
 (SELECT @id := 0) AS id
 ORDER BY kodebarang;

DROP TEMPORARY TABLE IF EXISTS temp_dataawal_paketmaterial;
CREATE TEMPORARY TABLE temp_dataawal_paketmaterial AS
SELECT
 @id := @id+1 AS idpaketmaterial,
 namapaket AS namapaketmaterial
FROM
 paketmaterial
 ,(SELECT @id := 0) AS id
 GROUP BY namapaket;

SELECT
 @idpdam,
 p.idpaketmaterial AS idpaketmaterial,
 idmaterial,
 pd.qty AS qty,
 0 AS ppn,
 NOW() AS waktuupdate
FROM
 paketmaterial pd
 JOIN temp_dataawal_paketmaterial p ON p.namapaketmaterial = pd.namapaket
 JOIN temp_dataawal_barang b ON b.kodematerial = pd.kodebarang