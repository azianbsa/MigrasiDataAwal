SELECT
@idpdam,
@id := @id + 1 AS iduser,
a.nama,
a.namauser,
a.passworduser,
a.aktif AS aktif,
NULL AS noidentitas,
-1 AS idrole,
-1 AS idloket,
0 AS flagbatasiwilayahpelayanan,
0 AS flaghapus,
NOW() AS waktuupdate
FROM (
SELECT nama,namauser,`passworduser`,alamat,aktif FROM [bacameter].`userakses`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,aktif FROM [bsbs].`userakses`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM [loket].`userloket`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM [loket].`userbshl`
) a,
(SELECT @id := 0) AS id
GROUP BY a.namauser