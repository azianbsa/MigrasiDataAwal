SET FOREIGN_KEY_CHECKS = 0;

REPLACE INTO master_attribute_pdam (`idpdam`, `namapdam`, `tipe`)
SELECT
@idpdam,
@namapdam,
'basic' AS tipe;

REPLACE INTO master_attribute_pdam_detail
SELECT
@idpdam,
`key`,
`value`,
`waktuupdate`
FROM
master_attribute_pdam_detail
WHERE idpdam = @idpdamcopy;

REPLACE INTO setting_configuration
SELECT
@idpdam,
`setting_id`,
`value`,
`waktuupdate`
FROM
setting_configuration
WHERE idpdam = @idpdamcopy;

REPLACE INTO setting_mobile
SELECT
@idpdam,
`idmobileitem`,
`value`,
`waktuupdate`
FROM
setting_mobile
WHERE idpdam = @idpdamcopy;

REPLACE INTO master_tarif_meterai (id,idpdam, kodeperiodemulaiberlaku)
VALUES
(1,@idpdam, '100001');

REPLACE INTO `master_user` (
idpdam,
iduser,
nama,
namauser,
passworduser,
aktif,
idrole
)
VALUES
(
@idpdam,
1,
'Bsa-Azian',
CONCAT ('azian', @idpdam),
'$2a$11$kAPalx0K4eCytIlUHvmdb.DIptN4.yydS.O6p5KWPgAqNQ4SzYTR6',
1,
1
);

SET FOREIGN_KEY_CHECKS = 1;