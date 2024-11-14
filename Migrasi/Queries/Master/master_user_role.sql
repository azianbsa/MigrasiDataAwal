SET @maxId = (SELECT IFNULL(MAX(idrole),0) FROM master_user_role);

SELECT
@idpdam,
@i := @i + 1,
namarole,
flaghapus,
waktuupdate
FROM master_user_role,
(SELECT @i := @maxId) AS i
WHERE idpdam = @idpdamcopy AND flaghapus = 0;