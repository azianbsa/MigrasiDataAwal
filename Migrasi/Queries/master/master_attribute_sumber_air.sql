SELECT
@idpdam,
@id:=@id+1 AS idsumberair,
kodesumberair,
sumberair AS namasumberair,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
sumberair,
(SELECT @id:=0) AS id
WHERE `kodesumberair`<>'-'
UNION ALL
SELECT
@idpdam,
-1 AS idsumberair,
'-' AS kodesumberair,
'-' AS namasumberair,
0 AS flaghapus,
NOW() AS waktuupdate