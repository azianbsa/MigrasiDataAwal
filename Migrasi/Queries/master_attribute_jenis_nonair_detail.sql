SELECT
 @idpdam,
 idjenisnonair,
 parameter,
 postbiaya,
 biaya,
 islocked,
 waktuupdate
FROM
 master_attribute_jenis_nonair_detail
WHERE idpdam=@idpdamcopy;