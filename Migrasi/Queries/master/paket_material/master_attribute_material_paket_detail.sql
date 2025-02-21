DROP TEMPORARY TABLE IF EXISTS __tmp_barang;
CREATE TEMPORARY TABLE __tmp_barang AS
SELECT
@idpdam,
@id := @id+1 AS idmaterial,
kodebarang AS kodematerial
FROM
barang,
(SELECT @id := 0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_paketmaterial;
CREATE TEMPORARY TABLE __tmp_paketmaterial AS
SELECT
@idpdam,
@id := @id+1 AS idpaketmaterial,
@id AS kodepaketmaterial,
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
JOIN __tmp_paketmaterial p ON p.namapaketmaterial = pd.namapaket
JOIN __tmp_barang b ON b.kodematerial = pd.kodebarang