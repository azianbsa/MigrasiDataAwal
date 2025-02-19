DROP TEMPORARY TABLE IF EXISTS __tmp_loket;
CREATE TEMPORARY TABLE __tmp_loket AS
SELECT
@id := @id + 1 AS idloket,
kodeloket
FROM
loket,
(SELECT @id := 0) AS id
ORDER BY kodeloket;

DROP TEMPORARY TABLE IF EXISTS __tmp_userloket;
CREATE TEMPORARY TABLE __tmp_userloket AS
SELECT
@id := @id + 1 AS iduser,
ul.nama
FROM
userloket ul
,(SELECT @id := 0) AS id
ORDER BY nama;

SELECT
d.id,
@idpdam,
na.`idangsuran` AS idangsuran,
na.`id` AS idnonair,
pel.id AS idpelangganair,
NULL AS idpelangganlimbah,
NULL AS idpelangganlltt,
d.`periode` AS kodeperiode,
d.`termin` AS termin,
d.`flaglunas` AS statustransaksi,
d.`noangsuran` AS nomortransaksi,
d.`waktubayar` AS waktutransaksi,
YEAR(d.waktubayar) AS tahuntransaksi,
us.iduser AS iduser,
lo.idloket AS idloket,
NULL AS idkolektiftransaksi,
NULL AS idalasanbatal,
d.ketjenis AS keterangan,
ryn.id AS idrayon,
NULL AS idkelurahan,
gol.id AS idgolongan,
NULL AS iddiameter,
d.tglmulaitagih AS tglmulaitagih,
IFNULL(d.biayabahan,0) AS biayabahan,
IFNULL(d.biayapasang,0) AS biayapasang,
0 AS ujl,
IFNULL(d.ppn,0) AS ppn,
IFNULL(d.pendaftaran,0) AS pendaftaran,
IFNULL(d.administrasi,0) AS administrasi,
IFNULL(d.meterai,0) AS meterai,
IFNULL(d.lainnya,0) AS lainnya,
IFNULL(d.jumlah,0) AS total,
IFNULL(d.`waktuupload`,NOW()) AS waktuupdate
FROM __tmp_nonair na
JOIN detailangsuran d ON d.`noangsuran`=na.`noangsuran1`
LEFT JOIN pelanggan pel ON pel.nosamb = na.dibebankankepada
LEFT JOIN [bsbs].rayon ryn ON ryn.koderayon = na.koderayon
LEFT JOIN [bsbs].golongan gol ON gol.kodegol = na.kodegol AND gol.aktif = 1
LEFT JOIN __tmp_userloket us ON us.nama = na.kasir
LEFT JOIN __tmp_loket lo ON lo.kodeloket = na.loketbayar