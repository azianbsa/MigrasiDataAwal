SELECT
@idpdam AS idpdam,
@id:=@id+1 AS iddiameter,
kodediameter AS kodediameter,
ukuran AS namadiameter,
periodemulaiberlaku AS periodemulaiberlaku,
aktif AS status,
'' AS nomorsk,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
diameter,
(SELECT @id:=0) AS id;