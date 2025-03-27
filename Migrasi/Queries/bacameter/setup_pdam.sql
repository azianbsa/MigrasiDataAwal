SET FOREIGN_KEY_CHECKS = 0;

INSERT INTO master_attribute_pdam (`idpdam`, `namapdam`, `tipe`)
SELECT
@idpdam,
@namapdam,
'basic' AS tipe;

INSERT INTO master_attribute_pdam_detail
SELECT
@idpdam,
`key`,
`value`,
`waktuupdate`
FROM
master_attribute_pdam_detail
WHERE idpdam = @idpdamcopy;

INSERT INTO setting_configuration
SELECT
@idpdam,
`setting_id`,
`value`,
`waktuupdate`
FROM
setting_configuration
WHERE idpdam = @idpdamcopy;

INSERT INTO setting_mobile
SELECT
@idpdam,
`idmobileitem`,
`value`,
`waktuupdate`
FROM
setting_mobile
WHERE idpdam = @idpdamcopy;

INSERT INTO master_tarif_meterai (idpdam, kodeperiodemulaiberlaku)
VALUES
(@idpdam, '100001');

INSERT INTO `master_user` (
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