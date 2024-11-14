-- DELETE FROM master_attribute_pdam WHERE idpdam = @idpdam;
-- INSERT INTO master_attribute_pdam
-- SELECT
--  @idpdam,
--  @namapdam,
--  '-' AS provinsi,
--  '-' AS kota,
--  '-' AS alamatlengkap,
--  'basic' AS tipe,
--  0 AS flaghapus,
--  NOW() waktuupdate;

-- DELETE FROM master_attribute_pdam_detail WHERE idpdam = @idpdam;
-- INSERT INTO master_attribute_pdam_detail
-- SELECT
--  @idpdam,
--  `key`,
--  `value`,
--  waktuupdate
-- FROM
--  master_attribute_pdam_detail
-- WHERE idpdam = @idpdamcopy;

-- DELETE FROM setting_configuration WHERE idpdam = @idpdam;
-- INSERT INTO setting_configuration
-- SELECT
--  @idpdam,
--  setting_id,
--  `value`,
--  waktuupdate
-- FROM
--  setting_configuration
-- WHERE idpdam = @idpdamcopy;

-- DELETE FROM setting_mobile WHERE idpdam = @idpdam;
-- INSERT INTO setting_mobile
-- SELECT
--  @idpdam,
--  idmobileitem,
--  VALUE,
--  waktuupdate
-- FROM
--  setting_mobile
-- WHERE idpdam = @idpdamcopy;

DELETE FROM setting_gcs WHERE idpdam = @idpdam;
INSERT INTO setting_gcs
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
WHERE idpdam = @idpdamcopy;

DELETE FROM master_tarif_meterai WHERE idpdam = @idpdam;
INSERT INTO master_tarif_meterai (idpdam,kodeperiodemulaiberlaku)
VALUES (@idpdam,'100001');

-- DELETE FROM master_user_role WHERE idpdam = @idpdam;
-- SET @maxId = (SELECT IFNULL(MAX(idrole),0) FROM master_user_role);
-- INSERT INTO master_user_role(idpdam,idrole,namarole)
-- SELECT @idpdam,@i := @i+1,namarole
-- FROM master_user_role,
-- (SELECT @i := @maxId) AS i
-- WHERE idpdam = @idpdamcopy AND flaghapus = 0;

-- DELETE FROM master_user_role_access WHERE idpdam = @idpdam;
-- INSERT INTO master_user_role_access (idpdam,idrole,idmoduleaccess,`value`)
-- SELECT @idpdam,c.idrole,a.idmoduleaccess,a.value
-- FROM (
-- 	SELECT idrole,idmoduleaccess,`value`
-- 	FROM master_user_role_access 
-- 	WHERE idpdam = @idpdamcopy AND flaghapus = 0
-- 	AND idrole IN (SELECT idrole FROM master_user_role WHERE idpdam = @idpdamcopy AND flaghapus = 0)
-- ) a
-- 
-- JOIN 
-- (SELECT idrole,namarole FROM master_user_role WHERE idpdam = @idpdamcopy AND flaghapus = 0) b
-- ON b.idrole = a.idrole
-- 
-- JOIN
-- (SELECT idrole,namarole FROM master_user_role WHERE idpdam = @idpdam AND flaghapus = 0) c
-- ON c.namarole = b.namarole;

DELETE FROM `master_user` WHERE idpdam = @idpdam;
SET @maxIdUser = (SELECT IFNULL(MAX(iduser),0)+1 FROM `master_user`);
INSERT INTO `master_user` (idpdam,iduser,nama,namauser,passworduser,aktif,idrole)
VALUES (
@idpdam,
@maxIdUser,
'Bsa-Azian',
CONCAT('azian',@idpdam),
'$2a$11$kAPalx0K4eCytIlUHvmdb.DIptN4.yydS.O6p5KWPgAqNQ4SzYTR6',
1,
(SELECT idrole FROM master_user_role WHERE idpdam = @idpdam AND flaghapus = 0 AND namarole = 'Administrator'));