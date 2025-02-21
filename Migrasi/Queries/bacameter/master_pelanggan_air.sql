DROP TEMPORARY TABLE IF EXISTS __tmp_golongan;
CREATE TEMPORARY TABLE __tmp_golongan AS
SELECT
@id:=@id+1 AS id,
kodegol,
aktif
FROM
golongan,
(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_diameter;
CREATE TEMPORARY TABLE __tmp_diameter AS
SELECT
@id:=@id+1 AS id,
kodediameter,
aktif
FROM
diameter,
(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_merkmeter;
CREATE TEMPORARY TABLE __tmp_merkmeter AS
SELECT
@id:=@id+1 AS id,
merk
FROM
merkmeter,
(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_kolektif;
CREATE TEMPORARY TABLE __tmp_kolektif AS
SELECT
@id:=@id+1 AS id,
kodekolektif
FROM
kolektif,
(SELECT @id:=0) AS id;

SELECT
@idpdam,
pel.id AS idpelangganair,
pel.nosamb,
pel.norekening,
pel.nama,
pel.alamat,
pel.rt,
pel.rw,
IFNULL(gol.id, -1) AS idgolongan,
IFNULL(dia.id, -1) AS iddiameter,
-1 AS idjenispipa,
IFNULL(mer.id, -1) AS idkwh,
IFNULL(ray.id, -1) AS idrayon,
IFNULL(kel.id, -1) AS idkelurahan,
IFNULL(kol.id, -1) AS idkolektif,
pel.status AS idstatus,
pel.flag AS idflag,
p.`latitude` AS latitude,
p.`longitude` AS longitude,
NULL AS alamatmap,
NULL AS fotorumah1,
NULL AS fotorumah2,
NULL AS fotorumah3,
NULL AS fotokwh,
NULL AS fotodenah1,
NULL AS fotodenah2,
999 AS akurasi,
NULL AS nosamblama,
flaghapus AS flaghapus,
COALESCE(pel.`tanggal_hapus`,NOW()) AS waktuupdate
FROM
pelanggan pel
LEFT JOIN [bacameter].`pelanggan` p ON p.`idpelanggan`=pel.nosamb
LEFT JOIN __tmp_golongan gol ON gol.kodegol = pel.kodegol AND gol.aktif = 1
LEFT JOIN __tmp_diameter dia ON dia.kodediameter = pel.kodediameter AND dia.aktif = 1
LEFT JOIN __tmp_merkmeter mer ON mer.merk = pel.merkmeter
LEFT JOIN rayon ray ON ray.koderayon = pel.koderayon
LEFT JOIN kelurahan kel ON kel.kodekelurahan = pel.kodekelurahan
LEFT JOIN __tmp_kolektif kol ON kol.kodekolektif = pel.kodekolektif;