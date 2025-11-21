SELECT
@idpdam,
@id:=@id+1 AS idkolektif,
kodekolektif,
kolektif AS namakolektif,
ket AS keterangan,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
kolektif,
(SELECT @id:=0) AS id;