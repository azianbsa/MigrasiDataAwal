SELECT
@idpdam,
@id:=@id+1 AS idperuntukan,
peruntukan AS namaperuntukan,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
`peruntukan`,
(SELECT @id:=0) AS id;