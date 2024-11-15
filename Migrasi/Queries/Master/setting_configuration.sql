SELECT
 @idpdam,
 setting_id,
 `value`,
 waktuupdate
FROM
 setting_configuration
WHERE idpdam = @idpdam;