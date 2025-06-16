SELECT
@idpdam AS idpdam,
@id:=@id+1 AS idkolektif,
kodekolektif AS kodekolektif,
kolektif AS namakolektif,
ket AS keterangan,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
kolektif,
(SELECT @id:=0) AS id
WHERE `kodekolektif`<>'-'
UNION ALL
SELECT
@idpdam,
-1 AS idkolektif,
'-' AS kodekolektif,
'-' AS namakolektif,
'-' AS keterangan,
0 AS flaghapus,
NOW() AS waktuupdate
