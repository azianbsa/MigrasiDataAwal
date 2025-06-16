SELECT
@idpdam AS idpdam,
@id:=@id+1 AS idgolongan,
kodegol AS kodegolongan,
golongan AS namagolongan,
kategori AS kategori,
uraian AS uraian,
'' AS nomorsk,
periodemulaiberlaku AS periodemulaiberlaku,
aktif AS status,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
golongan,
(SELECT @id:=0) AS id;