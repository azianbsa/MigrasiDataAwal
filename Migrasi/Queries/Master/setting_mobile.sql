SELECT
 @idpdam,
 idmobileitem,
 `value`,
 waktuupdate
FROM
 setting_mobile
WHERE idpdam = @idpdamcopy;