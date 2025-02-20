SELECT
@idpdam,
@id:=@id+1 AS `idtipependaftaransambungan`,
tipe AS `namatipependaftaransambungan`,
0 AS `flaghapus`,
NOW() AS `waktuupdate`
FROM `tipesambungan`,
(SELECT @id:=0) AS id