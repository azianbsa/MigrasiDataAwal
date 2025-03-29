DROP TEMPORARY TABLE IF EXISTS __tmp_sumberair;
CREATE TEMPORARY TABLE __tmp_sumberair AS
SELECT
@id:=@id+1 AS id,
kodesumberair,
sumberair
FROM
sumberair,
(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_merkmeter;
CREATE TEMPORARY TABLE __tmp_merkmeter AS
SELECT
@id:=@id+1 AS id,
merk
FROM
merkmeter,
(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_kondisimeter;
CREATE TEMPORARY TABLE __tmp_kondisimeter AS
SELECT
@id:=@id+1 AS id,
kodekondisi,
kondisi
FROM
kondisimeter
,(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_kepemilikanbangunan;
CREATE TEMPORARY TABLE __tmp_kepemilikanbangunan AS
SELECT
@id:=@id+1 AS id,
`kepemilikanbangunan`
FROM
`kepemilikan`,
(SELECT @id:=0) AS id;

SELECT
@idpdam,
p.id AS idpelangganair,
s.id AS idsumberair,
-1 AS iddma,
-1 AS iddmz,
b.id AS idblok,
m.id AS idmerekmeter,
k.id AS idkondisimeter,
adm.id AS idadministrasilain,
pem.id AS idpemeliharaanlain,
ret.id AS idretribusilain,
-1 AS idpekerjaan,
-1 AS idjenisbangunan,
-1 AS idperuntukan,
kb.id AS idkepemilikan,
p.nosegelmeter AS nosegel,
p.nohp AS nohp,
p.notelp AS notelp,
p.noktp AS noktp,
'' AS nokk,
p.email AS email,
p.serimeter AS noserimeter,
p.tglmeter AS tglmeter,
'' AS pekerjaan,
IF(p.penghuni='' OR p.penghuni IS NULL, 0, p.penghuni) AS penghuni,
IF(p.namapemilik='' OR p.namapemilik IS NULL, p.nama, p.namapemilik) AS namapemilik,
p.alamat AS alamatpemilik,
'' AS kodepost,
0 AS dayalistrik,
0 AS luastanah,
IF(p.luasrumah='' OR p.luasrumah IS NULL, 0, p.luasrumah) AS luasrumah,
IF(p.urutanbaca='' OR p.urutanbaca IS NULL, 0, p.urutanbaca) AS urutanbaca,
IF(p.stan_awal_pasang='' OR p.stan_awal_pasang IS NULL, 0, p.stan_awal_pasang) AS stanawalpasang,
LEFT(p.nopendaftaran, 30) AS nopendaftaran,
p.tgldaftar AS tgldaftar,
NULL AS tglpenentuanbaca,
LEFT(p.norab, 30) AS norab,
'' AS nobapemasangan,
p.tgldaftar AS tglpasang,
p.tglputus AS tglputus,
NULL AS noserimeterlama,
CASE WHEN p.kategori_putus=1 THEN 'Tunggakan' WHEN p.kategori_putus=2 THEN 'Tunggakan' WHEN p.kategori_putus=3 THEN 'Tunggakan' WHEN p.kategori_putus=4 THEN 'Permohonan' ELSE NULL END AS kategoriputus,
NULL AS idtipependaftaransambungan,
NULL AS idkategorikawasan,
p.keterangan AS keterangan,
p.waktuupdate AS waktuupdate
FROM
pelanggan p
LEFT JOIN __tmp_sumberair s ON s.kodesumberair=p.kodesumberair
LEFT JOIN __tmp_merkmeter m ON m.merk=p.merkmeter
LEFT JOIN __tmp_kondisimeter k ON k.kodekondisi=p.kodekondisimeter
LEFT JOIN __tmp_kepemilikanbangunan kb ON kb.kepemilikanbangunan=p.kepemilikanbangunan
LEFT JOIN [bsbs].blok b ON b.kodeblok=p.kodeblok
LEFT JOIN [bsbs].byadministrasi_lain adm ON adm.kode=p.kodeadministrasilain
LEFT JOIN [bsbs].bypemeliharaan_lain pem ON pem.kode=p.kodepemeliharaanlain
LEFT JOIN [bsbs].byretribusi_lain ret ON ret.kode=p.koderetribusilain