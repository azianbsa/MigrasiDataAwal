SELECT
@idpdam,
@id:=@id+1 AS idgolongan,
kodegol AS kodegolongan,
golongan AS namagolongan,
kategori,
uraian,
'' AS nomorsk,
periodemulaiberlaku,
aktif AS STATUS,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
golongan,
(SELECT @id:=0) AS id;