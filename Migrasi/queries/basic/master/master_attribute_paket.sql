DROP TEMPORARY TABLE IF EXISTS __tmp_paketmaterial;
CREATE TEMPORARY TABLE __tmp_paketmaterial AS
SELECT
@id := @id+1 AS idpaketmaterial,
namapaket AS namapaketmaterial
FROM
paketmaterial
,(SELECT @id := 0) AS id
GROUP BY namapaket;
 
DROP TEMPORARY TABLE IF EXISTS __tmp_paketongkos;
CREATE TEMPORARY TABLE __tmp_paketongkos AS
SELECT
@id := @id+1 AS idpaketongkos,
namapaket AS namapaketongkos
FROM
paketongkos
,(SELECT @id := 0) AS id
GROUP BY namapaket;

DROP TEMPORARY TABLE IF EXISTS __tmp_diameter;
CREATE TEMPORARY TABLE __tmp_diameter AS
SELECT
@id:=@id+1 AS id,
kodediameter,
aktif
FROM
diameter,
(SELECT @id:=0) AS id;

SELECT
@idpdam,
@id := @id+1 AS idpaket,
@id AS kodepaket,
rab.namapaket AS namapaket,
CASE
WHEN rab.tipe_rab=1 THEN 'SAMB.BARU'
WHEN rab.tipe_rab=2 THEN 'SAMB.KEMBALI'
WHEN rab.tipe_rab=3 THEN 'LAINNYA'
END AS kategoripaket,
pm.idpaketmaterial AS idpaketmaterial,
po.idpaketongkos AS idpaketongkos,
NULL AS idgolongan,
dm.id AS iddiameter,
NULL AS kategoriputus,
IFNULL(rab.batas_bawah, 0) AS batasbawah,
IFNULL(rab.batas_atas, 0) AS batasatas,
0 AS ppnmaterial,
0 AS ppnmaterialtambahan,
0 AS ppnongkos,
0 AS ppnongkostambahan,
0 AS persentasekeuntungan,
rab.persenjasadaribahan AS persentasejasadaribahan,
0 AS flagharganetmaterial,
rab.harga_net_material AS harganetmaterial,
0 AS flagharganetongkos,
rab.harga_net_ongkos AS harganetongkos,
0 AS harganetpaket,
0 AS flagkavling,
rab.aktif AS `status`,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
paketrab rab
LEFT JOIN __tmp_paketmaterial pm ON pm.namapaketmaterial = rab.paketmat
LEFT JOIN __tmp_paketongkos po ON po.namapaketongkos = rab.paketongkos
LEFT JOIN __tmp_diameter dm ON dm.kodediameter = rab.diameter AND dm.aktif = 1
,(SELECT @id := 0) AS id