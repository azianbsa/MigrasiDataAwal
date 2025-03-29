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
p.id AS idpelangganair,
p.nosamb AS nosamb,
p.norekening AS norekening,
p.nama AS nama,
p.alamat AS alamat,
p.rt AS rt,
p.rw AS rw,
g.id AS idgolongan,
d.id AS iddiameter,
-1 AS idjenispipa,
m.id AS idkwh,
r.id AS idrayon,
k.id AS idkelurahan,
kl.id AS idkolektif,
p.status AS idstatus,
p.flag AS idflag,
bp.`latitude` AS latitude,
bp.`longitude` AS longitude,
NULL AS alamatmap,
NULL AS fotorumah1,
NULL AS fotorumah2,
NULL AS fotorumah3,
NULL AS fotokwh,
NULL AS fotodenah1,
NULL AS fotodenah2,
999 AS akurasi,
NULL AS nosamblama,
p.flaghapus AS flaghapus,
p.waktuupdate AS waktuupdate
FROM
pelanggan p
LEFT JOIN [bacameter].`pelanggan` bp ON bp.`idpelanggan`=p.nosamb
LEFT JOIN [bsbs].rayon r ON r.koderayon=p.koderayon
LEFT JOIN [bsbs].kelurahan k ON k.kodekelurahan=p.kodekelurahan
LEFT JOIN __tmp_golongan g ON g.kodegol=p.kodegol AND g.aktif=1
LEFT JOIN __tmp_diameter d ON d.kodediameter=p.kodediameter AND d.aktif=1
LEFT JOIN __tmp_merkmeter m ON m.merk=p.merkmeter
LEFT JOIN __tmp_kolektif kl ON kl.kodekolektif=p.kodekolektif;