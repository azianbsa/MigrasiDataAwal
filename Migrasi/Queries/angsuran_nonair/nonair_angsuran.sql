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
@idpdam,
na.`idangsuran` AS idangsuran,
na.noangsuran1 AS noangsuran,
na.`id` AS idnonair,
jns.idjenisnonair AS idjenisnonair,
pel.id AS idpelangganair,
NULL AS idpelangganlimbah,
NULL AS idpelangganlltt,
na.nama AS nama,
na.alamat AS alamat,
na.notelp AS notelp,
na.nohp AS nohp,
d.waktudaftar AS waktudaftar,
na.`jumlahtermin` AS jumlahtermin,
d.`jumlahangsuranpokok` AS jumlahangsuranpokok,
d.`jumlahangsuranbunga` AS jumlahangsuranbunga,
d.`jumlahuangmuka` AS jumlahuangmuka,
d.`jumlah` AS total,
us.iduser AS iduser,
d.`tglmulaitagih` AS tglmulaitagihpertama,
na.nolpp AS noberitaacara,
NULL AS tglberitaacara,
1 AS flagpublish,
d.`waktudaftar` AS waktupublish,
d.`flaglunas` AS flaglunas,
d.`waktulunas` AS waktulunas,
0 AS flaghapus,
IFNULL(d.`waktudaftar`, NOW()) AS waktuupdate
FROM __tmp_nonair na
JOIN `daftarangsuran` d ON d.`id`=na.`idangsuran`
LEFT JOIN pelanggan pel ON pel.nosamb = na.dibebankankepada
LEFT JOIN __tmp_jenisnonair jns ON jns.kodejenisnonair = na.jenis
LEFT JOIN __tmp_userloket us ON us.nama = d.`userdaftar`