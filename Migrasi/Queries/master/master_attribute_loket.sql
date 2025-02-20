SELECT
@idpdam AS idpdam,
@id := @id + 1 AS idloket,
kodeloket,
loket AS namaloket,
NULL AS idwilayah,
flagmitra,
admmitra AS biayamitra,
aktif AS `status`,
1 AS idbank,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
loket,
(SELECT @id := 0) AS id