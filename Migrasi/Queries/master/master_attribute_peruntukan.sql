SELECT
@idpdam AS idpdam,
@id:=@id+1 AS idperuntukan,
peruntukan AS namaperuntukan,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
`peruntukan`,
(SELECT @id:=0) AS id
UNION ALL
SELECT
@idpdam AS idpdam,
-1 AS idperuntukan,
'-' AS namaperuntukan,
0 AS flaghapus,
NOW() AS waktuupdate