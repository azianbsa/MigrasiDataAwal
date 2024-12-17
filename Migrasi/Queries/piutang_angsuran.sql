DROP TEMPORARY TABLE IF EXISTS temp_dataawal_piutang_gelondongan;
CREATE TEMPORARY TABLE temp_dataawal_piutang_gelondongan AS
SELECT periode,nosamb
FROM piutang WHERE kode <> CONCAT(periode, '.', nosamb) GROUP BY periode, nosamb;

SELECT
 @idpdam,
 @id := @id+1 AS idangsuran,
 CONCAT(IFNULL(rek.noangsuran,''),'.',rek.kode) AS noangsuran,
 @jnsnonair AS idjenisnonair,
 pel.id AS idpelangganair,
 pel.nama AS nama,
 pel.alamat AS alamat,
 pel.notelp AS notelp,
 pel.nohp AS nohp,
 pel.tgldaftar AS waktudaftar,
 0 AS jumlahtermin,
 rek.total AS jumlahangsuranpokok,
 0 AS jumlahangsuranbunga,
 0 AS jumlahuangmuka,
 rek.total,
 NULL AS iduser,
 DATE_FORMAT(DATE_ADD(STR_TO_DATE(CONCAT(rek.periode,'01'), '%Y%m%d'), INTERVAL 1 MONTH), '%Y-%m-01') AS tglmulaitagihpertama,
 rek.nolpp AS noberitaacara,
 NULL AS tglberitaacara,
 1 AS flagpublish,
 now() AS waktupublish,
 0 AS flaglunas,
 NULL AS waktulunas,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM piutang rek
JOIN temp_dataawal_piutang_gelondongan gld ON gld.periode = rek.periode AND gld.nosamb = rek.nosamb
JOIN [bsbs].pelanggan pel ON pel.nosamb = rek.nosamb
,(SELECT @id := @lastid) AS id
WHERE rek.periode = @periode AND rek.kode = CONCAT(rek.periode, '.', rek.nosamb) AND rek.flagangsur = 1;