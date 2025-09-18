DROP TEMPORARY TABLE IF EXISTS __tmp_paketongkos;
CREATE TEMPORARY TABLE __tmp_paketongkos AS
SELECT
@id := @id+1 AS idpaketongkos,
namapaket AS namapaketongkos
FROM
paketongkos
,(SELECT @id := 0) AS id
GROUP BY namapaket;

SELECT
@idpdam,
p.idpaketongkos AS idpaketongkos,
o.id AS idongkos,
pd.qty AS qty,
0 AS ppn,
NOW() AS waktuupdate
FROM
paketongkos pd
JOIN __tmp_paketongkos p ON p.namapaketongkos = pd.namapaket
JOIN ongkos o ON o.kodeongkos = pd.kodeongkos