DROP TEMPORARY TABLE IF EXISTS temp_dataawal_user;
CREATE TEMPORARY TABLE temp_dataawal_user AS
SELECT
@id := @id + 1 AS iduser,
ul.nama
FROM
 [loket].userloket ul
 ,(SELECT @id := 0) AS id
 ORDER BY nama;

SELECT
 @idpdam,
 na.id AS idangsuran,
 na.nomor AS noangsuran,
 NULL AS idnonair,
 jns.id AS idjenisnonair,
 pel.id AS idpelangganair,
 NULL AS idpelangganlimbah,
 NULL AS idpelangganlltt,
 na.nama AS nama,
 na.alamat AS alamat,
 na.notelp AS notelp,
 na.nohp AS nohp,
 NULL AS waktudaftar,
 0 AS jumlahtermin,
 na.total AS jumlahangsuranpokok,
 0 AS jumlahangsuranbunga,
 0 AS jumlahuangmuka,
 na.total AS total,
 us.iduser AS iduser,
 na.tglmulaitagih AS tglmulaitagihpertama,
 na.nolpp AS noberitaacara,
 NULL AS tglberitaacara,
 1 AS flagpublish,
 NOW() AS waktupublish,
 0 AS flaglunas,
 NULL AS waktulunas,
 0 AS flaghapus,
 IFNULL(na.waktuupdate, NOW()) AS waktuupdate
FROM
 nonair na
 LEFT JOIN pelanggan pel ON pel.nosamb = na.dibebankankepada
 LEFT JOIN [loket].jenisnonair jns ON jns.jenis = na.jenis
 LEFT JOIN temp_dataawal_user us ON us.nama = na.kasir
 WHERE na.flagangsur = 1 AND na.flaghapus = 1 AND na.termin = 0 AND na.ketjenis NOT LIKE 'Uang_Muka%'