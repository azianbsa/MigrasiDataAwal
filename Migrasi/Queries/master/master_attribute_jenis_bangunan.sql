SELECT
@idpdam,
@id:=@id+1 AS idjenisbangunan,
jenis AS namajenisbangunan,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
`jenisbangunan`,
(SELECT @id:=0) AS id
WHERE `jenis`<>'-'
UNION ALL
SELECT
@idpdam,
-1 AS idjenisbangunan,
'-' AS namajenisbangunan,
0 AS flaghapus,
NOW() AS waktuupdate