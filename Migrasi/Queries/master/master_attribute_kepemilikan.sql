SELECT
@idpdam,
@id:=@id+1 AS idkepemilikan,
@id AS kodekepemilikan,
`kepemilikanbangunan` AS namakepemilikan,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
`kepemilikan`,
(SELECT @id:=0) AS id;