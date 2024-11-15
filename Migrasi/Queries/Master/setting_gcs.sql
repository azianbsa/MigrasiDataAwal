SELECT
 @idpdam,
 idgcssetting,
 credential,
 bucket,
 fotometerpath,
 flagaktif,
 flaghapus,
 waktuupdate
FROM
 setting_gcs
WHERE idpdam = @idpdam;