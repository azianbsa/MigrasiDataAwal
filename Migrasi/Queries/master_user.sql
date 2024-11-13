SELECT
 @idpdam,
 @id := @id + 1 AS iduser,
 ul.nama,
 ul.namauser,
 ul.passworduser,
 ul.flagaktif AS aktif,
 NULL AS noidentitas,
 -1 AS idrole,
 lo.idloket,
 0 AS flagbatasiwilayahpelayanan,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 userloket ul
 LEFT JOIN (
 SELECT
 @idloket := @idloket + 1 AS idloket,
 kodeloket
 FROM loket
 ,(SELECT @idloket := 0) AS idloket
 ORDER BY kodeloket
 ) lo ON lo.kodeloket = ul.kodeloket
 ,(SELECT @id := 0) AS id
 ORDER BY nama;