SELECT
@idpdam AS idpdam,
@id:=@id+1 AS iduser,
a.nama AS nama,
a.namauser AS namauser,
a.passworduser AS passworduser,
a.aktif AS aktif,
NULL AS noidentitas,
-1 AS idrole,
-1 AS idloket,
-1 AS idpegawai,
0 AS flagbatasiwilayahpelayanan,
0 AS flaghapus,
NOW() AS waktuupdate
FROM (
SELECT nama,namauser,`passworduser`,NULL AS alamat,aktif FROM [bacameter].`userakses`
UNION ALL
SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM [loket].`userloket`
UNION ALL
SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM [loket].`userbshl`
) a,
(SELECT @id:=0) AS id
GROUP BY a.nama