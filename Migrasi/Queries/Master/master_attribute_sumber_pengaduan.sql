SELECT
@idpdam,
@id:=@id+1 AS `idsumberpengaduan`,
sumberpengaduan AS `namasumberpengaduan`,
0 AS `flaghapus`,
NOW() AS `waktuupdate`
FROM `sumberpengaduan`,
(SELECT @id:=0) AS id