SELECT
 @idpdam,
 `key`,
 `value`,
 waktuupdate
FROM
 master_attribute_pdam_detail
WHERE idpdam = @idpdam;