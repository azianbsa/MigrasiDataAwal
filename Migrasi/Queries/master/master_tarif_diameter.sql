SELECT
@idpdam,
@id:=@id+1 AS iddiameter,
kodediameter,
ukuran AS namadiameter,
periodemulaiberlaku,
aktif AS STATUS,
'' AS nomorsk,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
diameter,
(SELECT @id:=0) AS id;