SELECT
@idpdam AS `idpdam`,
@id:=@id+1 AS `idurusan`,
0 AS `idsubdivisi`,
kodebagian AS `kodeurusan`,
namabagian AS `namaurusan`,
0 AS `urutanurusan`,
1 AS `flagaktif`,
0 AS `flaghapus`,
NOW() AS `waktuupdate`
FROM `pengaduan_bagian`
,(SELECT @id:=0) AS id
