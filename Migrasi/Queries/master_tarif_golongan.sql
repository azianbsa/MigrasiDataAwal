SELECT
 @idpdam,
 id AS idgolongan,
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
 golongan
 WHERE aktif=1